#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WIKI_DIR="${ROOT}/wiki"
WIKI_REPO="${WIKI_REPO:-${ROOT}/../log-forwarder.wiki}"

if [[ ! -d "${WIKI_DIR}" ]]; then
	echo "wiki source directory not found: ${WIKI_DIR}" >&2
	exit 1
fi

if [[ ! -d "${WIKI_REPO}/.git" ]]; then
	echo "Cloning wiki repo into ${WIKI_REPO} ..."
	git clone "https://github.com/sanjuthomas/log-forwarder.wiki.git" "${WIKI_REPO}"
fi

rsync -av --delete --exclude '.git' "${WIKI_DIR}/" "${WIKI_REPO}/"

(
	cd "${WIKI_REPO}"
	if git diff --quiet && git diff --cached --quiet; then
		echo "Wiki is already up to date."
		exit 0
	fi
	git add -A
	git commit -m "Sync wiki from repository"
	git push origin master
)

echo "Wiki synced to https://github.com/sanjuthomas/log-forwarder/wiki"
