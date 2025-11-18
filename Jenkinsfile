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
      }
    }
  }
  post {
    cleanup {
      deleteDir()  /* clean up our workspace */
    }
  }
}
