#!/bin/bash

if [ "$(uname)" == "Darwin" ]; then
	if ! hash greadlink 2>/dev/null; then
		echo "ERROR! GNU tools are required on OS X (brew install coreutils)"
		exit
	fi
fi

if [ "$(uname)" == "Darwin" ]; then
	HOOK_DIR=$(greadlink -f $( dirname "${BASH_SOURCE[0]}" ))
else
	HOOK_DIR=$(readlink -f $( dirname "${BASH_SOURCE[0]}" ))
fi

if [ -d "${HOOK_DIR}/../.git/hooks" ]; then
	echo 'moving "${HOOK_DIR}/../.git/hooks" to "${HOOK_DIR}/../.git/hooks.bak"'
	mv "${HOOK_DIR}/../.git/hooks" "${HOOK_DIR}/../.git/hooks.bak"
fi

ln -sf "${HOOK_DIR}" "${HOOK_DIR}/../.git/hooks"
