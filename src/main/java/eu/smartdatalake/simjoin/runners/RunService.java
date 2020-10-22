package eu.smartdatalake.simjoin.runners;

import org.springframework.boot.SpringApplication;

import eu.smartdatalake.simjoin.simjoinservice.ServiceApplication;

public class RunService {

	public RunService(String[] args) {

		String timeout = "";
		if (args.length == 3 && (args[1].equals("--timeout") || args[1].equals("-t")))
			timeout = args[2];

		System.setProperty("timeout", timeout);

		// START THE SERVICE
		SpringApplication.run(ServiceApplication.class, args);
	}
}