#!/usr/bin/env python3
"""
Git Divergent Branches Auto-Fixer
Automatically resolves common git issues like divergent branches
"""

import subprocess
import sys
import os
from pathlib import Path


class GitAutoFixer:
    def __init__(self):
        self.repo_path = Path.cwd()
        self.colors = {
            'GREEN': '\033[0;32m',
            'RED': '\033[0;31m',
            'YELLOW': '\033[1;33m',
            'BLUE': '\033[0;34m',
            'NC': '\033[0m'
        }

    def print_colored(self, message, color='BLUE'):
        """Print colored output"""
        c = self.colors.get(color, '')
        nc = self.colors['NC']
        print(f"{c}{message}{nc}")

    def run_command(self, cmd, capture=False):
        """Run shell command safely"""
        try:
            if capture:
                result = subprocess.run(
                    cmd,
                    shell=True,
                    capture_output=True,
                    text=True,
                    check=True
                )
                return result.stdout.strip()
            else:
                subprocess.run(cmd, shell=True, check=True)
                return True
        except subprocess.CalledProcessError as e:
            return None

    def check_git_repo(self):
        """Check if we're in a git repository"""
        result = self.run_command("git rev-parse --git-dir", capture=True)
        if result is None:
            self.print_colored("❌ Error: Not in a git repository!", 'RED')
            sys.exit(1)
        return True

    def has_uncommitted_changes(self):
        """Check for uncommitted changes"""
        result = self.run_command("git diff-index --quiet HEAD --", capture=True)
        return result is None

    def get_current_branch(self):
        """Get current branch name"""
        branch = self.run_command("git rev-parse --abbrev-ref HEAD", capture=True)
        return branch if branch else "unknown"

    def fix_divergent_branches(self):
        """Main fix for divergent branches"""
        self.print_colored("\n🔧 Git Divergent Branches Auto-Fixer", 'BLUE')
        self.print_colored("=" * 40, 'BLUE')
        
        # Check git repo
        self.check_git_repo()
        
        # Get branch info
        branch = self.get_current_branch()
        self.print_colored(f"\nCurrent branch: {branch}", 'YELLOW')
        
        # Check for uncommitted changes
        if self.has_uncommitted_changes():
            self.print_colored(
                "❌ Error: You have uncommitted changes.\n"
                "Please commit or stash your changes first.",
                'RED'
            )
            sys.exit(1)
        
        # Attempt pull with rebase
        self.print_colored(f"\nAttempting to pull from origin/{branch} with rebase...", 'YELLOW')
        self.print_colored("-" * 40)
        
        if self.run_command(f"git pull --rebase origin {branch}"):
            self.print_colored("\n✅ Success! Branches have been reconciled.", 'GREEN')
            self.print_colored("\nWhat was done:", 'GREEN')
            self.print_colored("  • Local commits rebased on top of remote branch", 'GREEN')
            self.print_colored("  • No merge commits created (cleaner history)", 'GREEN')
            
            # Offer to set default strategy
            self.print_colored("\nWould you like to set rebase as default? (y/n)", 'BLUE')
            response = input().strip().lower()
            
            if response == 'y':
                self.run_command("git config pull.rebase true")
                self.print_colored("✅ Default pull strategy set to rebase (local)", 'GREEN')
                self.print_colored("\nTo set globally: git config --global pull.rebase true", 'YELLOW')
        else:
            self.print_colored(
                "\n❌ Rebase failed. There may be conflicts.\n"
                "To resolve:\n"
                "  1. Fix conflicted files\n"
                "  2. Run: git add .\n"
                "  3. Run: git rebase --continue\n"
                "  4. Or abort: git rebase --abort",
                'RED'
            )
            sys.exit(1)
        
        self.print_colored("\nDone! 🎉\n", 'GREEN')

    def show_git_status(self):
        """Show git status summary"""
        self.print_colored("\n📊 Git Status Summary", 'BLUE')
        self.print_colored("-" * 40, 'BLUE')
        self.run_command("git status -s")

    def set_git_config_defaults(self):
        """Set recommended git config defaults"""
        self.print_colored("\n⚙️ Setting Git Configuration Defaults", 'BLUE')
        self.print_colored("-" * 40, 'BLUE')
        
        configs = {
            "pull.rebase": "true",
            "fetch.prune": "true",
            "core.autocrlf": "input",
        }
        
        for key, value in configs.items():
            self.run_command(f"git config {key} {value}")
            self.print_colored(f"✅ Set {key} = {value}", 'GREEN')
        
        self.print_colored("\nConfiguration updated for this repository.\n", 'GREEN')


def main():
    """Main entry point"""
    fixer = GitAutoFixer()
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "fix":
            fixer.fix_divergent_branches()
        elif command == "status":
            fixer.show_git_status()
        elif command == "config":
            fixer.set_git_config_defaults()
        else:
            print("Usage: python git_auto_fixer.py <command>")
            print("\nCommands:")
            print("  fix     - Fix divergent branches")
            print("  status  - Show git status")
            print("  config  - Set recommended git configs")
    else:
        # Default: fix divergent branches
        fixer.fix_divergent_branches()


if __name__ == "__main__":
    main()
