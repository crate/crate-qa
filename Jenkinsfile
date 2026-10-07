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
              set +e

              git submodule update --init

              rm -rf crate_src
              git clone https://github.com/crate/crate.git crate_src
              (cd crate_src && git checkout 0f7ca4b5eebe762eeb518e2a445252545a7d30e2)

              export CRATE_VERSION=$(pwd)/crate_src
              # export CRATE_HEAP_SIZE=1500m

              overall_status=0

              for i in $(seq 1 3); do
                echo "=== Run $i ==="

                rm -rf .venv
                uv venv --python 3.14
                source .venv/bin/activate
                uv pip install -U -e .

                (cd tests && python -m unittest discover -vvvf -s sqllogic)
                run_status=$?
                if [ "$run_status" -ne 0 ]; then
                  echo "Run $i failed with status $run_status"
                  overall_status=1
                fi

                deactivate
                rm -rf .venv
              done

              exit $overall_status
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
