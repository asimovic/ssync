if exist deploy (
	rd /s/q deploy
)
mkdir deploy
robocopy /mir . deploy/ /xd .git .idea __pycache__ logs deploy /xf .gitignore index.sqlite install.* b2_account_info

pause