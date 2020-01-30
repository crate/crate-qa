pipeline {
  agent any
  environment {
    JDK_11 = 'openjdk@1.11.0'
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
        stage('Python tests') {
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

              (cd tests && python -m unittest -vvvf)
            '''
          }
        }
        stage('Go client tests') {
          agent { label 'medium' }
          steps {
            checkout scm
            sh '''
              rm -rf env
              /usr/bin/python3.7 -m venv env
              source env/bin/activate
              python -m pip install -U cr8 crash
              jabba install $JDK_11
              JAVA_HOME=$(jabba which --home $JDK_11) tests/client_tests/go/run.sh
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
        stage('Nodejs client tests') {
          agent {
            dockerfile {
              label 'docker'
              filename 'tests/client_tests/node-postgres/Dockerfile'
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
              (cd tests/client_tests/node-postgres && npm install && ./run.sh)
            '''
          }
        }
      }
    }
  }
}
