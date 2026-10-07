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
            sh '''
              rm -rf .venv
              uv venv --python 3.14
              uv pip install mypy flake8
              find tests -name "*.py" | xargs .venv/bin/mypy --ignore-missing-imports
              find src -name "*.py" | xargs .venv/bin/mypy --ignore-missing-imports
              .venv/bin/flake8 src/ tests/
            '''
          }
        }
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
              (cd tests && python -m unittest discover -vvvf -s sqllogic)
            '''
          }
          post {
            always {
              archiveArtifacts artifacts: 'tests/sqllogic/sqllogic.log',
                               allowEmptyArchive: true
            }
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
