package main

import (
	"database_sync/utils"
	"fmt"
	"log"
	"sync"
	"time"
)

var (
	leadsSync    sync.Mutex
	budgetsSync  sync.Mutex
	ordersSync   sync.Mutex
	trackingSync sync.Mutex

	isLeadsSyncing    bool
	isBudgetsSyncing  bool
	isOrdersSyncing   bool
	isTrackingSyncing bool
)

func main() {
	utils.LoadEnvVariables()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		fmt.Println("=== Starting synchronization cycle ===")

		go func() {
			leadsSync.Lock()
			if isLeadsSyncing {
				fmt.Println("Leads synchronization already in progress, skipping...")
				leadsSync.Unlock()
				return
			}
			isLeadsSyncing = true
			leadsSync.Unlock()

			defer func() {
				leadsSync.Lock()
				isLeadsSyncing = false
				leadsSync.Unlock()
			}()

			fmt.Println("Running scheduled leads synchronization...")
			startTime := time.Now()
			if err := SyncLeads(); err != nil {
				log.Printf("Error synchronizing leads: %v", err)
			} else {
				elapsed := time.Since(startTime)
				fmt.Printf("Leads synchronization completed successfully (elapsed time: %s)\n", elapsed)
			}
		}()

		go func() {
			budgetsSync.Lock()
			if isBudgetsSyncing {
				fmt.Println("Budgets synchronization already in progress, skipping budgets and orders...")
				budgetsSync.Unlock()
				return
			}
			isBudgetsSyncing = true
			budgetsSync.Unlock()

			defer func() {
				budgetsSync.Lock()
				isBudgetsSyncing = false
				budgetsSync.Unlock()
			}()

			fmt.Println("Running scheduled budgets synchronization...")
			startTime := time.Now()
			if err := SyncBudgets(); err != nil {
				log.Printf("Error synchronizing budgets: %v", err)
				return
			} else {
				elapsed := time.Since(startTime)
				fmt.Printf("Budgets synchronization completed successfully (elapsed time: %s)\n", elapsed)
			}

			ordersSync.Lock()
			if isOrdersSyncing {
				fmt.Println("Orders synchronization already in progress, skipping...")
				ordersSync.Unlock()
				return
			}
			isOrdersSyncing = true
			ordersSync.Unlock()

			defer func() {
				ordersSync.Lock()
				isOrdersSyncing = false
				ordersSync.Unlock()
			}()

			fmt.Println("Running scheduled orders synchronization...")
			startTime = time.Now()
			if err := SyncOrders(); err != nil {
				log.Printf("Error synchronizing orders: %v", err)
			} else {
				elapsed := time.Since(startTime)
				fmt.Printf("Orders synchronization completed successfully (elapsed time: %s)\n", elapsed)
			}
		}()

		go func() {
			trackingSync.Lock()
			if isTrackingSyncing {
				fmt.Println("Orders tracking synchronization already in progress, skipping...")
				trackingSync.Unlock()
				return
			}
			isTrackingSyncing = true
			trackingSync.Unlock()

			defer func() {
				trackingSync.Lock()
				isTrackingSyncing = false
				trackingSync.Unlock()
			}()

			fmt.Println("Running scheduled orders tracking synchronization...")
			startTime := time.Now()
			if err := SyncOrdersTracking(); err != nil {
				log.Printf("Error synchronizing orders tracking: %v", err)
			} else {
				elapsed := time.Since(startTime)
				fmt.Printf("Orders tracking synchronization completed successfully (elapsed time: %s)\n", elapsed)
			}
		}()
	}
}
