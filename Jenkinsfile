pipeline {

	agent any
	stages {
		stage('[Schedule Sync] Start') {
			steps {
				 sh 'echo "Hello"'
				// slackSend(channel: '#deployment-alert', color: '#00FF7F' , message: "[Schedule Sync] Start : Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
			}
		}
		stage('[Schedule Sync] Git clone') {
                        steps {
				sh 'echo "Hello"'
				sh 'hostname'
				sh 'whoami'			
				sh 'ls -al /home/jenkins_home/och_aws.pem'				
//sh 'ssh -i /home/jenkins_home/och_aws.pem ubuntu@ec2-3-105-131-178.ap-southeast-2.compute.amazonaws.com ls'
                                // slackSend(channel: '#deployment-alert', color: '#00FF7F' , message: "[Schedule Sync] Git clone : Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
                        }
                }
		stage('[Schedule Sync] Done') {
                        steps {
				 sh 'echo "Hello"'
                                // slackSend(channel: '#deployment-alert', color: '#00FF7F' , message: "[Schedule Sync] Done : Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
                        }
                }
		
	}
}
