#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WIKI_DIR="${ROOT}/wiki"

if [[ ! -d "${WIKI_DIR}" ]]; then
	echo "wiki source directory not found: ${WIKI_DIR}" >&2
	exit 1
fi

wiki_repo_default="${ROOT}/../log-forwarder.wiki"
if [[ "${GITHUB_ACTIONS:-}" == "true" && -n "${RUNNER_TEMP:-}" ]]; then
	wiki_repo_default="${RUNNER_TEMP}/log-forwarder.wiki"
fi
WIKI_REPO="${WIKI_REPO:-${wiki_repo_default}}"

wiki_remote() {
	if [[ -n "${WIKI_REMOTE_URL:-}" ]]; then
		echo "${WIKI_REMOTE_URL}"
		return
	fi
	if [[ -n "${GITHUB_REPOSITORY:-}" ]]; then
		local host="https://github.com"
		if [[ -n "${GITHUB_TOKEN:-}" ]]; then
			host="https://x-access-token:${GITHUB_TOKEN}@github.com"
		fi
		echo "${host}/${GITHUB_REPOSITORY}.wiki.git"
		return
	fi
	echo "https://github.com/sanjuthomas/log-forwarder.wiki.git"
}

if [[ -d "${WIKI_REPO}/.git" ]]; then
	git -C "${WIKI_REPO}" remote set-url origin "$(wiki_remote)"
	git -C "${WIKI_REPO}" fetch origin
else
	echo "Cloning wiki repo into ${WIKI_REPO} ..."
	git clone "$(wiki_remote)" "${WIKI_REPO}"
fi

rsync -av --delete --exclude '.git' "${WIKI_DIR}/" "${WIKI_REPO}/"

(
	cd "${WIKI_REPO}"
	if git diff --quiet && git diff --cached --quiet; then
		echo "Wiki is already up to date."
		exit 0
	fi

	if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
		git config user.name "${GIT_AUTHOR_NAME:-github-actions[bot]}"
		git config user.email "${GIT_AUTHOR_EMAIL:-github-actions[bot]@users.noreply.github.com}"
	fi

	git add -A
	commit_msg="${WIKI_COMMIT_MSG:-Sync wiki from repository}"
	if [[ "${GITHUB_ACTIONS:-}" == "true" && -n "${GITHUB_SHA:-}" ]]; then
		commit_msg="Sync wiki from ${GITHUB_SHA}"
	fi
	git commit -m "${commit_msg}"
	git push origin HEAD:master
)

echo "Wiki synced to https://github.com/${GITHUB_REPOSITORY:-sanjuthomas/log-forwarder}/wiki"
