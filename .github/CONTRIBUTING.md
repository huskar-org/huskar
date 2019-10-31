## Contributing Guide

1. Keep your branch up-to-date
    - `git fetch upstream`
    - `git rebase upstream/master`
2. Squash or reword commits for clear history
    - `git rebase -i --autosquash upstream/master`
3. Create a pull request
    - Edit the description
    - Invite reviewers
    - Assign someone who can merge pull requests
    - Choose labels and milestone
    - If there is a topic-related issue, attach it

**IMPORTANT** The `master` branch should **be always deployable**.
Once you break the `master` via a pull request, choose a quickest way,
**revert it** or **fix it** as soon as possible.

The regression test must be included in all hotfix pull requests.
