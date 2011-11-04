
all : 
	mvn assembly:assembly -DdistVersion=0.1.`git log --pretty=format:'%ad' --date=short -n 1| tr -d "-"`

clean :
	mvn clean
