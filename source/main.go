package main

import (
	"database_sync/source/utils"
	"fmt"
	"log"
	"sync"
	"time"
)

func main() {
	utils.LoadEnvVariables()

	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		startTime := time.Now()
		if err := SyncUsers(); err != nil {
			log.Printf("Error synchronizing users: %v", err)
		} else {
			elapsed := time.Since(startTime)
			fmt.Printf("Initial user synchronization completed successfully (elapsed time: %s)\n", elapsed)
		}
	}()

	go func() {
		defer wg.Done()
		startTime := time.Now()
		if err := SyncLeads(); err != nil {
			log.Printf("Error synchronizing leads: %v", err)
		} else {
			elapsed := time.Since(startTime)
			fmt.Printf("Initial leads synchronization completed successfully (elapsed time: %s)\n", elapsed)
		}
	}()

	wg.Wait()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		wg.Add(2)

		go func() {
			defer wg.Done()
			fmt.Println("Running scheduled user synchronization...")
			startTime := time.Now()
			if err := SyncUsers(); err != nil {
				log.Printf("Error synchronizing users: %v", err)
			} else {
				elapsed := time.Since(startTime)
				fmt.Printf("User synchronization completed successfully (elapsed time: %s)\n", elapsed)
			}
		}()

		go func() {
			defer wg.Done()
			fmt.Println("Running scheduled leads synchronization...")
			startTime := time.Now()
			if err := SyncLeads(); err != nil {
				log.Printf("Error synchronizing leads: %v", err)
			} else {
				elapsed := time.Since(startTime)
				fmt.Printf("Leads synchronization completed successfully (elapsed time: %s)\n", elapsed)
			}
		}()

		wg.Wait()
	}
}
