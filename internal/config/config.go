package config

// API holds the API gateway's settings.
type API struct {
	ListenAddr string
}

func APIConfig() API {
	return API{
		ListenAddr: getenv("API_LISTEN_ADDR", ":8080"),
	}
}


