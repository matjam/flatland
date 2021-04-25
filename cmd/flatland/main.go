package main

import (
	"github.com/matjam/flatland/internal/cache"
)

func main() {
	objectCache := cache.New()

	objectCache.Import("data/5m_sales_records.csv")
}
