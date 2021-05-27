
INSTALL INSTRUCTIONS for UAT
=============================================

1) Go to this SharePoint location and download tdcoa.zip:
    https://teradata.sharepoint.com/teams/SalesTech/COA/Collections/UAT_Application/tdcoa.zip

2) Unzip to a directory to somewhere on your local drive

3) Double-Click the appropriate "command" file, depending on your OS
   - Windows   = "win--run.cmd"
   - Apple Mac = "mac--run.command"

4) Wait while the installer does stuff
   - the very first time you run, it will take a few minutes as
     python sets up a virtual environment at that location
     (you'll see a ./env/ folder in that directory)
   - it'll also download and install all dependencies

5) Once the application pops up, you can close the terminal window
   (windows will close automatically, mac may remain open)



Troubleshooting - Reasons you might encounter errors
-----------------------------------------------------
If something goes wrong, here are some high-level checks to perform:

a) Is Python 3.8.x or higher installed?   If not, jump to a2).  If you think
   python IS installed, try the test:
   test: on commandline, type> python --version
   expecting: Python 3.8.x  (or higher)

  a1) if the test returns an error, or a lower version of Python than 3.8.x
       try: type>  python3 --version
       try: type>  python3.8 --version
              or>  python3.9 --version
              or>  py --version
                  (rare, but some old windows machines have this abbreviation)

      if typing an alternate "python" name returns the expected value, then you
      have python installed, but the PATH environment variable is incorrect.
      You can either:
       - modify your environment variable PATH to include the location to your
         python3.8 installation (good choice if you use python for other uses)
          or
       - edit the COA command path (#3 above), changing every line that currently
         calls "python" to instead call "python3" or whatever worked during your
         test (easiest /fastest choice)

  a2) if every trial of> "python --version" fails no matter what you try, you
      probably don't have python installed.  Go to this link and download the
      64bit version of python for your operating system:
      https://www.python.org/downloads/
      - current version is 3.9, anything over 3.8 will work
      - this install is very easy /wizard driven, but if you have questions or
        need guidance, this is an excellent walk-thru:
        https://docs.python.org/3/using/windows.html
      - make sure you get the 64bit version(!)  COA will not work with 32bit
      - while doing the install, there will be a checkbox to "add to PATH"
        this is recommended, as it will prevent issues like a1)

  a3) if you know you have Anaconda setup, but you cannot get the expected
      results above... well, you're in for some wrestling.  While not
      recommended, Anaconda should work - it just takes some changes to
      the PATH environment variable and to the COA startup script.  For the UAT,
      we're not going to cover this use-case, but please let us know if this is
      a hard blocker for you, and we'll craft an Anaconda-specific solution.
