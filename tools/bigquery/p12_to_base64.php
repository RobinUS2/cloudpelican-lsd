<?php
$path = trim($argv[1]);
if (empty($path)) {
	echo 'ERR: Please provide a path to the P12 key file' . "\n";
	exit(1);
}

$content = file_get_contents($path);
echo base64_encode($content);
