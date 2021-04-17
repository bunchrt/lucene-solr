#!/usr/bin/env bash

selection=$1



if [ "${selection}" = "startup" ]; then
  cat "${HOME}/logconfig.current" | xsel -i -b
fi

