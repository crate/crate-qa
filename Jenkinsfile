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
      }
    }
  }
}
