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
      }
    }
  }
  post {
    cleanup {
      deleteDir()  /* clean up our workspace */
    }
  }
}
