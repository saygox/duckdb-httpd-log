#!/bin/bash
BRANCH=$(git branch --show-current)
if [ "$BRANCH" != "develop" ]; then
    echo "❌ Error: This worktree should be on 'develop' branch, but on '$BRANCH'"
    exit 1
fi

if [ -f "docs/review/httpd_log_review_guidelines.md" ]; then
    echo "❌ Error: Review guidelines should not exist in develop branch"
    exit 1
fi

if [ ! -f "docs/httpd_log_extension_requirements.md" ]; then
    echo "❌ Error: Requirements document not found"
    exit 1
fi

echo "✅ Branch validated: $BRANCH"
echo "📄 Using: docs/httpd_log_extension_requirements.md"

