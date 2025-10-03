// https://www.jenkins.io/doc/pipeline/tour/agents/
// https://www.jenkins.io/doc/book/pipeline/docker/
// https://www.jenkins.io/doc/book/pipeline/syntax/
pipeline {
  agent any
  options {
    timeout(time: 4, unit: 'HOURS')
  }
  stages {
    stage('Parallel') {
      parallel {
        stage('Lint python code') {
          agent { label 'medium && x64' }
          steps {
            checkout scm
            sh 'rm -rf env'
            sh '/usr/bin/python3.11 -m venv env'
            sh 'env/bin/python -m pip install -U mypy flake8'
            sh 'find tests -name "*.py" | xargs env/bin/mypy --ignore-missing-imports'
            sh 'find src -name "*.py" | xargs env/bin/mypy --ignore-missing-imports'
            sh 'env/bin/flake8 src/ tests/'
          }
        }
        stage('Python bwc-rolling-upgrade tests') {
          agent { label 'medium && x64' }
          tools { jdk 'jdk11' }
          steps {
            checkout scm
            sh '''
              rm -rf env
              /usr/bin/python3.11 -m venv env
              source env/bin/activate
              python -m pip install -U -e .

              (cd tests/bwc && python -m unittest -vvvf test_rolling_upgrade.py)
            '''
          }
        }
        stage('Python bwc-hotfix_downgrades tests') {
          agent { label 'medium && x64' }
          steps {
            checkout scm
            sh '''
              rm -rf env
              /usr/bin/python3.11 -m venv env
              source env/bin/activate
              python -m pip install -U -e .

              (cd tests/bwc && python -m unittest -vvvf test_hotfix_downgrades.py)
            '''
          }
        }
        stage('Python bwc-upgrade tests') {
          agent { label 'medium && x64' }
          tools { jdk 'jdk11' }
          steps {
            checkout scm
            sh '''
              rm -rf env
              /usr/bin/python3.11 -m venv env
              source env/bin/activate
              python -m pip install -U -e .

              (cd tests/bwc && python -m unittest -vvvf test_upgrade.py)
            '''
          }
        }
        stage('Python restart tests') {
          agent { label 'medium && x64' }
          steps {
            checkout scm
            sh '''
              rm -rf env
              /usr/bin/python3.11 -m venv env
              source env/bin/activate
              python -m pip install -U -e .

              (cd tests && python -m unittest discover -vvvf -s restart)
            '''
          }
        }
        stage('Python startup tests') {
          agent { label 'medium && x64' }
          steps {
            checkout scm
            sh '''
              rm -rf env
              /usr/bin/python3.11 -m venv env
              source env/bin/activate
              python -m pip install -U -e .

              (cd tests && python -m unittest discover -vvvf -s startup)
            '''
          }
        }
        stage('Python sqllogic tests') {
          agent { label 'medium && x64' }
          steps {
            checkout scm
            sh '''
              rm -rf env
              /usr/bin/python3.11 -m venv env
              source env/bin/activate
              python -m pip install -U -e .

              git submodule update --init
              (cd tests && python -m unittest discover -vvvf -s sqllogic)
            '''
          }
        }
        stage('Python client tests') {
          agent { label 'medium && x64' }
          steps {
            checkout scm
            sh '''
              rm -rf env
              /usr/bin/python3.11 -m venv env
              source env/bin/activate
              python -m pip install -U -e .

              (cd tests && python -m unittest discover -vvvf -s client_tests)
            '''
          }
        }
        stage('Go client tests') {
          agent {
            dockerfile {
              label 'docker && x64'
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
              cd ${HOME}/tests/client_tests/go
              test -f go.mod && rm go.mod
              test -f go.sum && rm go.sum
              ./run.sh
            '''
          }
        }
        stage('Haskell client tests') {
          agent {
            dockerfile {
              label 'docker && x64'
              filename 'tests/client_tests/haskell/Dockerfile'
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
              python -m pip install -U cr8==0.27.2
              ./tests/client_tests/haskell/run.sh
            '''
          }
        }
        stage('Stock JDBC tests') {
          agent { label 'medium && x64' }
          tools { jdk 'jdk11' }
          steps {
            checkout scm
            sh '''
              (cd tests/client_tests/stock_jdbc && ./gradlew test)
            '''
          }
        }
        stage('Rust client tests') {
          agent {
            dockerfile {
              label 'docker && x64'
            }
          }
          steps {
            sh '''
              export HOME=$(pwd)
              export LANG=en_US.UTF-8
              test -d env && rm -rf env
              python3 -m venv env
              . env/bin/activate
              python -m pip install -U cr8==0.27.2
              (cd tests/client_tests/rust/ && ./run.sh)
            '''
          }
        }
        stage('Node.js client tests') {
          agent {
            dockerfile {
              label 'docker && x64'
              filename 'tests/client_tests/node-postgres/Dockerfile'
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
              test -d env && rm -rf env
              python3 -m venv env
              . env/bin/activate
              python -m pip install -U cr8

              # Get ready.
              cd tests/client_tests/node-postgres

              # Install test prerequisites.
              npm install --verbose

              # CrateDB needs a locale setting.
              export LANG=en_US.UTF-8
              ./run.sh
            '''
          }
        }
        stage('npgsql client tests') {
          agent {
            dockerfile {
              label 'docker && x64'
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
              python -m pip install -U cr8==0.27.2
              (cd tests/client_tests/stock_npgsql && ./run.sh)
            '''
          }
        }
        stage('kafka-connect jdbc tests') {
          agent {
            label 'docker && medium && x64'
          }
          steps {
            checkout scm
            sh '''
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
