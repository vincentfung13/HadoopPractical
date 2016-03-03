# UGBigDataAssignmentOne

This is our private repo for the big data assignment. The following sections are about how to set up the project and work on your own branch.

### Setting up

To get the code, navigate to your workspace directory and type in the command line and do:
```
git clone git@github.com:vincentfung13/HadoopPractical.git
```

This will download the code to your workspace directory in a new folder HadoopPractical.

To work on eclipse, do File->New->Java Project to create a new project, then in the project creation window, untick "Use default location" and choose the HadoopPractical folder. 

Then you will have to manually add the provided hadoop jar files to the project build path. You can do so by simply right click the project, choose Build Ptah->Configure Build Path, click on "Add External Jars" and add all the hadoop jars. This should get the code to compile.

### Working on your own branch

Each of us should create our own branch and work from there in order to keep the master branch clean. You can create your own branch in the command line:

```
git pull origin master
git checkout -b Your-New-Branch-Name
git push origin Your-New-Branch-Name
```

To update your local repository (syncing with what everyone else has done), simply do:
```
git pull origin <the-branch-name-you-are-updating-from>
```

To commit and push your changes to your branch, do the following:
```
<!--First check the changes made since the last commit-->
git status

<!--Add the files you want to commit to the "ready" zone-->
<!--To add all the changes to the repo, do "git add --all"-->
git add the-file-you-want-to-add

<!--Commit-->
git commit -m "your-commit-message"

<!--Push the changes to remote repo-->
git push origin Your-New-Branch-Name
```

To merge your branch to master, in your own branch, commit all your local changes, do the following:
```
<!--CAUTION: DO NOT PUSH NOT WORKING CODE TO MASTER-->
<!--Swich to master-->
git checkout master

<!--Update your local repo from master, and fix any conflicts that appear-->
git pull origin master

<!--Merge and push-->
git merge Your-Branch-Name
git push origin master
```

Here are a couple of useful commands:
```
<!--If you want to review your changes again-->
git diff the-file-you-changed

<!--Discard changes in a file-->
git checkout the-file-you-changed
```

Do remember to pull every time before you start working!

Happy coding :)
