#!/usr/bin/env python3
"""
Git Safety Checker - Ensures sensitive files are properly ignored
Run this before committing to verify no sensitive data will be pushed
"""

import os
import subprocess
import sys
from pathlib import Path

def check_env_files():
    """Check for environment files that should be ignored"""
    env_patterns = [
        '.env', '.env.*', '*.env', 
        'api_keys.*', 'secrets.*', 'credentials.*',
        '*password*', '*token*', '*key*'
    ]
    
    found_files = []
    for pattern in env_patterns:
        for file_path in Path('.').glob(pattern):
            if file_path.is_file():
                found_files.append(str(file_path))
    
    return found_files

def check_git_status():
    """Check what files git would commit"""
    try:
        # Get staged files
        result = subprocess.run(['git', 'diff', '--cached', '--name-only'], 
                              capture_output=True, text=True, check=True)
        staged_files = result.stdout.strip().split('\n') if result.stdout.strip() else []
        
        # Get untracked files
        result = subprocess.run(['git', 'ls-files', '--others', '--exclude-standard'], 
                              capture_output=True, text=True, check=True)
        untracked_files = result.stdout.strip().split('\n') if result.stdout.strip() else []
        
        return staged_files, untracked_files
    except subprocess.CalledProcessError:
        return [], []

def check_sensitive_content(file_path):
    """Check if file contains sensitive content"""
    sensitive_keywords = [
        'api_key', 'secret', 'password', 'token', 'credentials',
        'POLYGON_API_KEY', 'AWS_ACCESS_KEY', 'AWS_SECRET',
        'BUCKET_NAME', 'SNS_TOPIC_ARN'
    ]
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read().lower()
            for keyword in sensitive_keywords:
                if keyword.lower() in content:
                    return True
    except Exception:
        pass
    
    return False

def main():
    print("🔍 Git Safety Check - Protecting Your Secrets")
    print("=" * 50)
    
    # Check if we're in a git repository
    if not os.path.exists('.git'):
        print("❌ Not in a git repository")
        return
    
    # Check for environment files
    env_files = check_env_files()
    if env_files:
        print("🔍 Found environment/sensitive files:")
        for file in env_files:
            print(f"  📄 {file}")
    
    # Check git status
    staged_files, untracked_files = check_git_status()
    
    # Check staged files for sensitive content
    dangerous_staged = []
    for file in staged_files:
        if file and os.path.exists(file):
            if any(pattern in file.lower() for pattern in ['.env', 'secret', 'key', 'password']):
                dangerous_staged.append(file)
            elif check_sensitive_content(file):
                dangerous_staged.append(file)
    
    # Check untracked files
    dangerous_untracked = []
    for file in untracked_files:
        if file and os.path.exists(file):
            if any(pattern in file.lower() for pattern in ['.env', 'secret', 'key', 'password']):
                dangerous_untracked.append(file)
    
    # Report results
    print(f"\n📊 Status Report:")
    print(f"  Staged files: {len(staged_files)}")
    print(f"  Untracked files: {len(untracked_files)}")
    
    if dangerous_staged:
        print(f"\n🚨 DANGER! Staged files with sensitive content:")
        for file in dangerous_staged:
            print(f"  ❌ {file}")
        print(f"\n💡 To unstage these files:")
        for file in dangerous_staged:
            print(f"  git reset HEAD {file}")
        return False
    
    if dangerous_untracked:
        print(f"\n⚠️  Untracked files that should be ignored:")
        for file in dangerous_untracked:
            print(f"  📄 {file}")
        print(f"\n💡 These files are not staged, but ensure .gitignore is correct")
    
    # Check if .gitignore exists
    if not os.path.exists('.gitignore'):
        print(f"\n⚠️  No .gitignore file found!")
        print(f"💡 Create a .gitignore file to protect sensitive files")
        return False
    
    # Check if .env is in .gitignore
    with open('.gitignore', 'r') as f:
        gitignore_content = f.read()
    
    if '.env' not in gitignore_content:
        print(f"\n⚠️  .env not found in .gitignore!")
        print(f"💡 Add '.env' to your .gitignore file")
        return False
    
    if dangerous_staged:
        print(f"\n❌ COMMIT BLOCKED - Sensitive files detected!")
        return False
    else:
        print(f"\n✅ Safe to commit - No sensitive files detected in staging area")
        return True

if __name__ == "__main__":
    is_safe = main()
    sys.exit(0 if is_safe else 1)