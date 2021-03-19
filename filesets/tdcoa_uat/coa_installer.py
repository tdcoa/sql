import os, sys, shutil, subprocess
from pathlib import Path

# Set current directory to location of this script
path_script = Path(os.path.dirname(os.path.abspath(__file__)))
print('this script location: %s' %path_script)

# Install path is grandparent of current script location
path_tdcoa = Path(path_script.parent.parent.parent.absolute() / 'tdcoa')
if not os.path.exists(path_tdcoa): os.mkdir(path_tdcoa)
print('target path location: %s' %path_tdcoa)
if not os.path.exists(path_tdcoa / 'log'): os.mkdir(path_tdcoa / 'log')
if not os.path.exists(path_tdcoa / 'wheel'): os.mkdir(path_tdcoa / 'wheel')

# copy all files to new location
for filename in os.listdir(path_script):
    if not filename.endswith(".py"):
        src = Path(path_script / filename)
        dst = Path(path_tdcoa  / filename)
        if filename.endswith('.whl'): dst = Path(path_tdcoa  / 'wheel' / filename)
        if not os.path.exists(dst):
            shutil.copyfile(src, dst)

print("\n\nFiles successfully copied!  Horray Technology!")

os.chdir(path_tdcoa.absolute())
if sys.platform == "darwin":  #MacOS
    cmdfile = 'mac--run.command'
else: # Windows - i don't care about Linux, sorry
    cmdfile = 'win--run.cmd'

print("Starting the application...")
os.system('chmod 777 %s' %Path(path_tdcoa / cmdfile).absolute())
os.system(Path(path_tdcoa / cmdfile).absolute())
