# ssync
A secure backup tool that encrypts files locally before sending them to the cloud. Based on the Backblaze B2 command line tool.
This project only supports Backblaze B2 Cloud Storage.

## Setup
#### Key Generation - Windows
- Install gpg4win https://www.gpg4win.org/index.html
- Run Kleopatra from gpg4win
- Create a new personal certificate
- Right click on the certificate and export the private key with ascii armor

#### Configuration (ssync.conf)
- `gpgkeyfile` location of the gpg private key file
- `accountid` b2 account id
- `applicationkey` b2 application key
<pre>
Example:
gpgkeyfile = C:\backup.asc

[RemoteB2]
accountid = ad987c6df6e8
applicationkey = 9a87fd87a6587f6c589768d7a64f76d4g7a6987ac3
</pre>

## Running ssync
Backup files to cloud
<pre>
python ssync.py -s c:\MyFolder\ -d b2:\\mybackup passphrase_for_certificate
</pre>
Download files from cloud
<pre>
python ssync.py -s b2:\\mybackup -d c:\MyFolder\ passphrase_for_certificate
</pre>
Command line argument help
<pre>
python ssync.py --help
</pre>
