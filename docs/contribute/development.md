# Development Guide

The following guide will help you quickly run Firehose in your local machine.
The main components of Firehose are:

* Consumer: Handles data consumption from Kafka.
* Sink: Package which handles sinking data.

## Requirements

### Development environment
The following software is required for Firehose development

* Java SE Development Kit 8

### Services
The following components/services are required to develop Firehose:
* Kafka > 2.4 to consume messages from.
* Corrsponding sink service to sink data to.

## Style Guide

### Java
We conform to the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html). Maven can helpfully take care of that for you before you commit:

## Making a pull request

#### Incorporating upstream changes from master
Our preference is the use of git rebase instead of git merge.
Signing commits

```sh
# Include -s flag to signoff
$ git commit -s -m "feat: my first commit"
```

Good practices to keep in mind
* Follow the [conventional commit](https://www.conventionalcommits.org/en/v1.0.0/) format for all commit messages.
* Fill in the description based on the default template configured when you first open the PR
* Include kind label when opening the PR
* Add WIP: to PR name if more work needs to be done prior to review
* Avoid force-pushing as it makes reviewing difficult