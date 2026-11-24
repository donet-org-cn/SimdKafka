#!/usr/bin/env bash
set -euo pipefail

image=$1
release_tag=$2
images=$3
version=${release_tag#v}
version_ref="$image:$version"
latest_ref="$image:latest"

verify_platforms() {
    jq -e '[.manifests[].platform | "\(.os)/\(.architecture)"] | sort == ["linux/amd64","linux/arm64"]' >/dev/null
}

error=$(mktemp)
trap 'rm -f "$error"' EXIT
if version_manifest=$(docker buildx imagetools inspect --raw "$version_ref" 2>"$error"); then
    verify_platforms <<< "$version_manifest"
    printf 'Using the existing multi-architecture tag %s\n' "$version_ref"
else
    if ! grep -Eqi 'manifest unknown|not found' "$error"; then
        cat "$error" >&2
        exit 1
    fi

    references=()
    for arch in amd64 arm64; do
        local_ref="simdkafka-ci:$arch"
        arch_ref="$image:$version-$arch"
        docker image load --input "$images/$arch.tar"
        docker image tag "$local_ref" "$arch_ref"
        digest=$(docker image push "$arch_ref" | tee /dev/stderr |
            sed -nE 's/.*digest: (sha256:[0-9a-f]{64}) .*/\1/p')
        [[ $digest =~ ^sha256:[0-9a-f]{64}$ ]]
        references+=("$image@$digest")
    done
    docker buildx imagetools create --tag "$version_ref" "${references[@]}"
    version_manifest=$(docker buildx imagetools inspect --raw "$version_ref")
    verify_platforms <<< "$version_manifest"
fi

latest_release=$(gh api "repos/$GITHUB_REPOSITORY/releases/latest" --jq .tag_name)
if [[ $release_tag == "$latest_release" ]]; then
    if ! latest_manifest=$(docker buildx imagetools inspect --raw "$latest_ref" 2>"$error"); then
        if ! grep -Eqi 'manifest unknown|not found' "$error"; then
            cat "$error" >&2
            exit 1
        fi
        latest_manifest=
    fi
    if [[ $latest_manifest != "$version_manifest" ]]; then
        docker buildx imagetools create --tag "$latest_ref" "$version_ref"
        [[ $(docker buildx imagetools inspect --raw "$latest_ref") == "$version_manifest" ]]
    fi
    printf 'Verified %s and %s\n' "$version_ref" "$latest_ref"
else
    printf 'Verified %s. GitHub currently marks %s as latest.\n' "$version_ref" "$latest_release"
fi
