How to use your new CLI toolkit
Now your script behaves like a standard command-line utility. Here are a few ways you can run it:

Standard Run (Defaults to processing):
python library_manager.py

Safe Testing (Overrides the JSON to ensure no files are moved):
python library_manager.py --dry-run

Database Maintenance Only (Cleans dead links and updates hashes, but doesn't scan the input folder):
python library_manager.py --prune --prepopulate

The Full Pipeline (Cleans DB, hashes old files, then processes new ones):
python library_manager.py --prune --prepopulate --process

Use a Different Config (Great if you want to test on a different directory):
python library_manager.py -c test_config.json

This setup keeps your day-to-day command short, but leaves the powerful maintenance tools just a flag away.