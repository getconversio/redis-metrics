#!/bin/env bash
# Prepare documentation for gh-pages push
# Check the updates with "git status" before pushing.
rm -rf out/
npm run-script docs
git checkout gh-pages
git pull
rm *.html
rm -rf scripts/
rm -rf fonts/
rm -rf styles/
cp -r out/* .
git add .
