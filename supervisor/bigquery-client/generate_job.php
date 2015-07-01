<?php
$projectId = $argv[1]; // Example: my-cinema-12345
$datasetId = $argv[2]; // Example: cloudpelican_lsd (you have to create this yourself)
$serviceAccount = $argv[3]; // Name of your service account, example: 1234-abc@developer.gserviceaccount.com
$keyFilePath = $argv[4]; // Path to your Google PK12 key file
$query = $argv[5]; // Example: select count(1) from your-table

// Read key
$keyData = base64_encode(file_get_contents($keyFilePath));

// To json
$json = json_encode(
                array(
                        'project_id' => $projectId,
                        'dataset_id' => $datasetId,
                        'service_account_id' => $serviceAccount,
                        'pk12base64' => $keyData,
                        'query' => $query
                )
        );
echo $json . "\n\n";

// To conf
echo base64_encode($json) ."\n\n";
