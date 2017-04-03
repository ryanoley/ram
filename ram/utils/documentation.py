import os


def prompt_for_description():
    desc = raw_input("\nEnter a description of this run:\n")
    if len(desc) == 0:
        print('\nMust enter description!!\n')
        desc = prompt_for_description()
    return desc


def get_git_branch_commit():
    """
    This is used for documenting where the simulation came from.
    """
    repo_dir = os.path.join(os.getenv('GITHUB'), 'ram')
    git_branch = open(os.path.join(repo_dir, '.git/HEAD'), 'r').read()
    git_branch = git_branch.split('/')[-1].replace('\n', '')
    git_commit = os.path.join(repo_dir, '.git/refs/heads', git_branch)
    git_commit = open(git_commit).read().replace('\n', '')
    return git_branch, git_commit
