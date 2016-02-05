# UGBigDataAssignmentOne

This is our private repo for the big data assignment. The following sections are about how to set up the project and work on your own branch.

### Setting up

To get the code, navigate to your workspace directory and type in the command:
```
git clone git@github.com:vincentfung13/UGBigDataAssignmentOne.git
```

This will download the code to your workspace directory in a new folder UGBigDataAssignmentOne.

To work on eclipse, do File->New->Java Project to create a new project, then in the project creation window, untick "Use default location" and choose the UGBigDataAssignmentOne folder. 

Then you will have to manually add the provided hadoop jar files to the project build path. You can do so by simply right click the project, choose Build Ptah->Configure Build Path, click on "Add External Jars" and add all the hadoop jars. This should get the code to compile.

### Working on your own branch

Each of us should create our own branch and work from there in order to keep the master branch clean. You can create your own branch in the command line:

```
git pull origin master
git checkout -b Your-New-Branch-Name
git push origin Your-New-Branch-Name
```

To commit and push your changes, do the following:
```
<!--First check the changes made since the last commit-->
git status

<!--Add the files you want to commit to the "ready" zone-->
<!--To add all the changes to the repo, do "git add *"-->
git add the-file-you-want-to-add

<!--Commit-->
git commit -m "your-commit-message"

<!--Push the changes to remote repo-->
git push origin Your-New-Branch-Name
```

Here are a couple of useful commands:
```
<!--If you want to review your changes again-->
git diff the-file-you-changed

<!--Discard changes in a file-->
git checkout the-file-you-changed
```

Do remember to pull every time you start working!

Happy coding :)
