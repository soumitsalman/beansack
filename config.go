package main

import "os"

func getDBConnectionString() string {
	// log.Println(os.Getenv("DB_CONNECTION_STRING"))
	return os.Getenv("DB_CONNECTION_STRING")
}

func getParrotBoxUrl() string {
	return os.Getenv("PARROTBOX_URL")
}

func getInternalAuthToken() string {
	return os.Getenv("INTERNAL_AUTH_TOKEN")
}

func getLLMServiceAPIKey() string {
	return os.Getenv("LLMSERVICE_API_KEY")
}
