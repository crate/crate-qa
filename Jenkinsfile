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
        stage('Python bwc-rolling-upgrade tests 5-5') {
          agent { label 'medium && x64' }
          tools { jdk 'jdk11' }
          steps {
            checkout scm
            sh '''
              rm -rf .venv
              uv venv --python 3.14
              source .venv/bin/activate
              uv pip install -U -e .

              (cd tests/bwc && python -m unittest -vvv test_rolling_upgrade.RollingUpgradeTest.test_rolling_upgrade_5_to_5)
            '''
          }
        }
        stage('Python bwc-rolling-upgrade tests 5-6') {
          agent { label 'medium && x64' }
          tools { jdk 'jdk17' }
          steps {
            checkout scm
            sh '''
              rm -rf .venv
              uv venv --python 3.14
              source .venv/bin/activate
              uv pip install -U -e .

              (cd tests/bwc && python -m unittest -vvv test_rolling_upgrade.RollingUpgradeTest.test_rolling_upgrade_5_to_6)
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
