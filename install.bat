if not exist deploy (
	mkdir deploy
)
robocopy /mir . deploy/ /xd .git .idea __pycache__ logs deploy gpg /xf .gitignore index.sqlite install.* b2_account_info

pause