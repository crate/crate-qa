// https://www.jenkins.io/doc/pipeline/tour/agents/
// https://www.jenkins.io/doc/book/pipeline/docker/
// https://www.jenkins.io/doc/book/pipeline/syntax/
pipeline {
  agent any
  environment {
    JDK_11 = 'openjdk@1.11.0'
  }
  options {
    timeout(time: 4, unit: 'HOURS') 
  }
  stages {
    stage('Parallel') {
      parallel {
        stage('Lint python code') {
          agent { label 'medium' }
          steps {
            checkout scm
            sh 'rm -rf env'
            sh '/usr/bin/python3.7 -m venv env'
            sh 'env/bin/python -m pip install -U mypy flake8'
            sh 'find tests -name "*.py" | xargs env/bin/mypy --ignore-missing-imports'
            sh 'find src -name "*.py" | xargs env/bin/mypy --ignore-missing-imports'
            sh 'env/bin/flake8 src/ tests/'
          }
        }
        stage('Python bwc tests') {
          agent { label 'medium' }
          steps {
            checkout scm
            sh '''
              rm -rf env
              /usr/bin/python3.7 -m venv env
              source env/bin/activate
              python -m pip install -U -e .

              jabba install $JDK_11
              export JAVA_HOME=$(jabba which --home $JDK_11)

              (cd tests && python -m unittest discover -vvvf -s bwc)
            '''
          }
        }
        stage('Python restart tests') {
          agent { label 'medium' }
          steps {
            checkout scm
            sh '''
              rm -rf env
              /usr/bin/python3.7 -m venv env
              source env/bin/activate
              python -m pip install -U -e .

              jabba install $JDK_11
              export JAVA_HOME=$(jabba which --home $JDK_11)

              (cd tests && python -m unittest discover -vvvf -s restart)
            '''
          }
        }
        stage('Python startup tests') {
          agent { label 'medium' }
          steps {
            checkout scm
            sh '''
              rm -rf env
              /usr/bin/python3.7 -m venv env
              source env/bin/activate
              python -m pip install -U -e .

              jabba install $JDK_11
              export JAVA_HOME=$(jabba which --home $JDK_11)

              (cd tests && python -m unittest discover -vvvf -s startup)
            '''
          }
        }
        stage('Python sqllogic tests') {
          agent { label 'medium' }
          steps {
            checkout scm
            sh '''
              rm -rf env
              /usr/bin/python3.7 -m venv env
              source env/bin/activate
              python -m pip install -U -e .

              jabba install $JDK_11
              export JAVA_HOME=$(jabba which --home $JDK_11)

              git submodule update --init
              (cd tests && python -m unittest discover -vvvf -s sqllogic)
            '''
          }
        }
        stage('Python client tests') {
          agent { label 'medium' }
          steps {
            checkout scm
            sh '''
              rm -rf env
              /usr/bin/python3.7 -m venv env
              source env/bin/activate
              python -m pip install -U -e .

              jabba install $JDK_11
              export JAVA_HOME=$(jabba which --home $JDK_11)

              (cd tests && python -m unittest discover -vvvf -s client_tests)
            '''
          }
        }
        stage('Go client tests') {
          agent {
            dockerfile {
              label 'docker'
              filename 'tests/client_tests/go/Dockerfile'
            }
          }
          steps {
            checkout scm
            sh '''
              export HOME=$(pwd)
              export LANG=en_US.UTF-8
              test -d env && rm -rf env
              python3 -m venv env
              . env/bin/activate
              python -m pip install -U cr8 crash
              (cd tests/client_tests/go && ./run.sh)
            '''
          }
        }
        stage('Haskell client tests') {
          agent { label 'medium' }
          steps {
            checkout scm
            sh '''
              rm -rf env
              /usr/bin/python3.7 -m venv env
              source env/bin/activate
              python -m pip install -U cr8
              jabba install $JDK_11
              mkdir -p ~/.local/bin
              export PATH=$HOME/.local/bin:$PATH
              curl -L https://www.stackage.org/stack/linux-x86_64 | tar xz --wildcards --strip-components=1 -C ~/.local/bin '*/stack'
              JAVA_HOME=$(jabba which --home $JDK_11) tests/client_tests/haskell/run.sh
            '''
          }
        }
        stage('Stock JDBC tests') {
          agent { label 'medium' }
          steps {
            checkout scm
            sh '''
              jabba install $JDK_11
              export JAVA_HOME=$(jabba which --home $JDK_11)
              (cd tests/client_tests/stock_jdbc && ./gradlew test)
            '''
          }
        }
        stage('Rust client tests') {
          agent {
            dockerfile {
              label 'docker'
            }
          }
          steps {
            sh '''
              export HOME=$(pwd)
              export LANG=en_US.UTF-8
              test -d env && rm -rf env
              python3 -m venv env
              . env/bin/activate
              python -m pip install -U cr8
              (cd tests/client_tests/rust/ && ./run.sh)
            '''
          }
        }
        stage('Node.js client tests') {
          agent {
            dockerfile {
              label 'docker'
              filename 'tests/client_tests/node-postgres/Dockerfile'

              // Run container as root user.
              // Note: This is needed to upgrade `node-gyp`.
              // https://stackoverflow.com/questions/53090408/jenkins-pipeline-build-inside-container
              // https://stackoverflow.com/questions/44805076/setting-build-args-for-dockerfile-agent-using-a-jenkins-declarative-pipeline
              args '--user=root:root'
            }
          }
          steps {

            // We need to explicitly checkout from SCM here.
            checkout scm

            echo "Running job ${env.JOB_NAME}"

            sh label: 'Invoking test recipe', script: '''

              # Environment information.
              echo "Hostname: $(hostname -f)"
              echo "Node.js version: $(node --version)"

              # Setup `cr8`.
              # Note: We don't use a virtualenv here as it looks like
              #       it screws something up with the following procedure.
              #       - https://github.com/nodejs/node-gyp/pull/1815
              #       - https://github.com/nodejs/node-gyp/issues/2144
              pip3 install --upgrade cr8

              # Upgrade `node-gyp`.
              # https://github.com/nodejs/node-gyp/issues/2272
              # https://stackoverflow.com/questions/44633419/no-access-permission-error-with-npm-global-install-on-docker-image
              # https://github.com/npm/npm/issues/16766#issuecomment-377950849
              npm config set unsafe-perm=true
              npm --global config set user root
              npm install --global node-gyp@7.1.2
              npm config set node_gyp $(npm prefix -g)/lib/node_modules/node-gyp/bin/node-gyp.js

              # Get ready.
              cd tests/client_tests/node-postgres

              # Install test prerequisites.
              npm install --verbose

              # Prepare environment for CrateDB.
              # CrateDB must not be run as `root`.
              useradd -m testdrive
              export HOME=$(pwd)

              # CrateDB needs a locale setting.
              export LANG=en_US.UTF-8

              # Invoke test suite.
              su testdrive ./run.sh
            '''
          }
        }
        stage('npgsql client tests') {
          agent {
            dockerfile {
              label 'docker'
              filename 'tests/client_tests/stock_npgsql/Dockerfile'
            }
          }
          steps {
            checkout scm
            sh '''
              export HOME=$(pwd)
              export LANG=en_US.UTF-8
              test -d env && rm -rf env
              python3 -m venv env
              . env/bin/activate
              python -m pip install -U cr8
              (cd tests/client_tests/stock_npgsql && ./run.sh)
            '''
          }
        }
        stage('kafka-connect jdbc tests') {
          agent {
            label 'docker'
          }
          steps {
            checkout scm
            sh '''
              jabba install $JDK_11
              export JAVA_HOME=$(jabba which --home $JDK_11)
              (cd tests/kafka-connect-jdbc/ && ./run.sh)
            '''
          }
        }
      }
    }
  }
  post {
    cleanup {
      deleteDir()  /* clean up our workspace */
    }
  }
}
