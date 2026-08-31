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
        stage('Python sqllogic tests') {
          agent { label 'medium && x64' }
          steps {
            checkout scm
            sh '''
              rm -rf .venv
              uv venv --python 3.14
              source .venv/bin/activate
              uv pip install -U -e .

              git submodule update --init
              export CRATE_VERSION=6.4.3
              # export CRATE_HEAP_SIZE=1200m
              (cd tests && python -m unittest discover -vvvf -s sqllogic)
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
