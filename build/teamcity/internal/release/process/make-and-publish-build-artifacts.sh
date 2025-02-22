#!/usr/bin/env bash

set -euxo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"  # for run_bazel

tc_start_block "Variable Setup"

build_name=$(git describe --tags --dirty --match=v[0-9]* 2> /dev/null || git rev-parse --short HEAD;)

# On no match, `grep -Eo` returns 1. `|| echo""` makes the script not error.
release_branch="$(echo "$build_name" | grep -Eo "^v[0-9]+\.[0-9]+" || echo"")"
is_custom_build="$(echo "$TC_BUILD_BRANCH" | grep -Eo "^custombuild-" || echo "")"

if [[ -z "${DRY_RUN}" ]] ; then
  bucket="cockroach-builds"
  gcs_bucket="cockroach-builds-artifacts-prod"
  google_credentials=$GOOGLE_COCKROACH_CLOUD_IMAGES_COCKROACHDB_CREDENTIALS
  gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb/cockroach"
  # Used for docker login for gcloud
  gcr_hostname="us-docker.pkg.dev"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_PROD"
else
  bucket="cockroach-builds-test"
  gcs_bucket="cockroach-builds-artifacts-dryrun"
  google_credentials="$GOOGLE_COCKROACH_RELEASE_CREDENTIALS"
  gcr_repository="us.gcr.io/cockroach-release/cockroach-test"
  build_name="${build_name}.dryrun"
  gcr_hostname="us.gcr.io"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_DEV"
fi
download_prefix="https://storage.googleapis.com/$gcs_bucket"

cat << EOF

  build_name:      $build_name
  release_branch:  $release_branch
  is_custom_build: $is_custom_build
  bucket:          $bucket
  gcs_bucket:      $gcs_bucket
  gcr_repository:  $gcr_repository

EOF
tc_end_block "Variable Setup"


# Leaving the tagging part in place to make sure we don't break any scripts
# relying on the tag.
tc_start_block "Tag the release"
git tag "${build_name}"
tc_end_block "Tag the release"

tc_start_block "Compile and publish S3 artifacts"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH=$build_name -e bucket=$bucket -e gcs_credentials -e gcs_bucket=$gcs_bucket" run_bazel << 'EOF'
bazel build --config ci //pkg/cmd/publish-provisional-artifacts
BAZEL_BIN=$(bazel info bazel-bin --config ci)
export google_credentials="$gcs_credentials"
source "build/teamcity-support.sh"  # For log_into_gcloud
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"
$BAZEL_BIN/pkg/cmd/publish-provisional-artifacts/publish-provisional-artifacts_/publish-provisional-artifacts -provisional -release -bucket "$bucket" --gcs-bucket="$gcs_bucket"
EOF
tc_end_block "Compile and publish S3 artifacts"

tc_start_block "Make and push multiarch docker images"
configure_docker_creds
docker_login_with_google

gcr_tag="${gcr_repository}:${build_name}"
declare -a docker_manifest_amends

for platform_name in "${platform_names[@]}"; do
  tarball_arch="$(tarball_arch_from_platform_name "$platform_name")"
  docker_arch="$(docker_arch_from_platform_name "$platform_name")"
  linux_platform=linux
  if [[ $tarball_arch == "aarch64" ]]; then
    linux_platform=linux-3.7.10-gnu
  fi
  # TODO: update publish-provisional-artifacts with option to leave one or more cockroach binaries in the local filesystem
  # NB: tar usually stops reading as soon as it sees an empty block but that makes
  # curl unhappy, so passing `--ignore-zeros` will cause it to read to the end.
  cp --recursive "build/deploy" "build/deploy-${docker_arch}"
  curl \
    --fail \
    --silent \
    --show-error \
    --output /dev/stdout \
    --url "${download_prefix}/cockroach-${build_name}.${linux_platform}-${tarball_arch}.tgz" \
    | tar \
    --directory="build/deploy-${docker_arch}" \
    --extract \
    --file=/dev/stdin \
    --ungzip \
    --ignore-zeros \
    --strip-components=1
  cp --recursive licenses "build/deploy-${docker_arch}"
  # Move the libs where Dockerfile expects them to be
  mv build/deploy-${docker_arch}/lib/* build/deploy-${docker_arch}/
  rmdir build/deploy-${docker_arch}/lib

  build_docker_tag="${gcr_repository}:${docker_arch}-${build_name}"
  docker build --no-cache --pull --platform "linux/${docker_arch}" --tag="${build_docker_tag}" "build/deploy-${docker_arch}"
  docker push "$build_docker_tag"
  docker_manifest_amends+=("--amend" "${build_docker_tag}")
done

docker manifest create "${gcr_tag}" "${docker_manifest_amends[@]}"
docker manifest push "${gcr_tag}"
tc_end_block "Make and push multiarch docker images"



# Make finding the tag name easy.
cat << EOF


Build ID: ${build_name}


EOF
