2. How the process works
The script follows a specific pipeline to ensure accuracy and prevent file errors.

Fingerprinting: fpcalc generates a unique digital signature for your audio file.

API Lookup: That signature is sent to AcoustID's servers to find a match.

Sanitization: The script removes characters like / \ : * ? " < > | which aren't allowed in filenames.

FileSystem Update: The os.rename function updates the local file on your drive.

3. Safety Tips
Backup First: Before running a bulk renaming script, always keep a copy of your music in a separate folder.

Pathing: Ensure fpcalc is either in your environment's Scripts folder or in the same folder as renamer.py.

Rate Limiting: If you have thousands of files, consider adding import time and time.sleep(1) inside the loop to avoid being temporarily blocked by the AcoustID API.