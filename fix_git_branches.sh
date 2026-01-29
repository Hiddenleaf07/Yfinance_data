#!/bin/bash

# Auto-fix Git divergent branches issue
# This script automatically resolves divergent branches by rebasing

set -e

echo "🔧 Git Divergent Branches Auto-Fixer"
echo "===================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get current branch
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo -e "${YELLOW}Current branch: ${NC}$CURRENT_BRANCH"

# Check if there are uncommitted changes
if ! git diff-index --quiet HEAD --; then
    echo -e "${RED}❌ Error: You have uncommitted changes.${NC}"
    echo "Please commit or stash your changes before running this script."
    exit 1
fi

echo ""
echo "Attempting to pull from origin/$CURRENT_BRANCH with rebase..."
echo ""

# Try to pull with rebase strategy
if git pull --rebase origin "$CURRENT_BRANCH"; then
    echo ""
    echo -e "${GREEN}✅ Success! Branches have been reconciled.${NC}"
    echo ""
    echo "What was done:"
    echo "  • Local commits rebased on top of remote branch"
    echo "  • No merge commits created (cleaner history)"
    echo ""
else
    echo ""
    echo -e "${RED}❌ Rebase failed. There may be conflicts.${NC}"
    echo ""
    echo "To resolve conflicts manually:"
    echo "  1. Open conflicted files and fix them"
    echo "  2. Run: git add ."
    echo "  3. Run: git rebase --continue"
    echo "  4. To abort rebase: git rebase --abort"
    exit 1
fi

# Optional: Set default pull strategy to avoid this in future
echo ""
echo "Would you like to set rebase as your default pull strategy? (y/n)"
read -r RESPONSE

if [[ "$RESPONSE" == "y" || "$RESPONSE" == "Y" ]]; then
    git config pull.rebase true
    echo -e "${GREEN}✅ Default pull strategy set to rebase${NC}"
    echo "   (This was set only for this repository)"
    echo ""
    echo "To set globally for all repositories, run:"
    echo "  git config --global pull.rebase true"
fi

echo ""
echo "Done! 🎉"
