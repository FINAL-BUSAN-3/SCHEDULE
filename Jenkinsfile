pipeline {

	agent any
	stages {
		stage('[Schedule Sync] Start') {
			steps {
				sh 'echo "[Schedule Sync] Start"'
				slackSend(channel: '#deployment-alert', color: '#00FF7F' , message: "[Schedule Sync] Start : Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
			}
		}
		stage('[Schedule Sync] Git clone') {
                        steps {
				script{
					def localUser = 'jenkins'
					def localHost = '192.168.0.8'
					def pemPath = '/var/jenkins_home/server-key.pem'

					sh 'echo "[Schedule Sync] Git clone"'
					sh """
					ssh -i ${pemPath} ${localUser}@${localHost} "cd /mnt/c/User/dhcks/airflow/dags/SCHEDULE && git pull"
					"""
                                	slackSend(channel: '#deployment-alert', color: '#00FF7F' , message: "[Schedule Sync] Git clone : Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
				}
			}
                }
		stage('[Schedule Sync] Done') {
                        steps {
				sh 'echo "[Schedule Sync] Done"'
                                slackSend(channel: '#deployment-alert', color: '#00FF7F' , message: "[Schedule Sync] Done : Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
			}
                }
		
	}
}
