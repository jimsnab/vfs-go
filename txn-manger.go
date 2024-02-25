package vfs

import (
	"sync"
	"sync/atomic"
)

type (
	transactionManager struct {
		dataStores []transactionDataStore
		onComplete CommitCompleted
		resolving  atomic.Bool
	}

	transactionCommit struct {
		mu        sync.Mutex
		resolvers []transactionResolver
	}

	transactionDataStore interface {
		// Register gives the resolver the chance to register any number of instances
		// to commit the transaction.
		Register(tc *transactionCommit) (err error)

		// Detach is used to let the data store know the transaction finished
		Detach()
	}

	transactionResolver interface {

		// Flush lays down the data on disk but does not release the recovery backup
		Flush() (err error)

		// Commit is called after all resolvers have successfully flushed
		Commit() (err error)

		// A Backout() function seems appropriate for failures but not implemented here
	}
)

func newTransactionManager(onComplete CommitCompleted) *transactionManager {
	return &transactionManager{
		dataStores: []transactionDataStore{},
		onComplete: onComplete,
	}
}

// Attaches a data store to the transaction manager for later resolution of the transaction
func (tm *transactionManager) Attach(tds transactionDataStore) {
	tm.dataStores = append(tm.dataStores, tds)
}

// Starts a go routine to perform flush and commit.
//
//   - If completionFn is non-nil, Resolve() will return nil err, and the
//     commit activity will run in a separate goroutine. When commit
//     completes, completeFn is called with the result.
//
// - If completionFn is nil, Resolve() will block and return the result in err.
//
// If fault is non-nil, no resolvers will be called; only data stores will be detached.
func (tm *transactionManager) Resolve(fault error) (err error) {
	if tm.resolving.Swap(true) {
		panic("already resolving")
	}

	if fault != nil {
		for _, dataStore := range tm.dataStores {
			dataStore.Detach()
		}
		return fault
	}

	// ask all the data stores to bind their resolvers
	tc := transactionCommit{
		resolvers: []transactionResolver{},
	}

	for _, dataStore := range tm.dataStores {
		err = dataStore.Register(&tc)
		if err != nil {
			if tm.onComplete != nil {
				go tm.onComplete(err)
			}
			return
		}
	}

	// if nil was specified for onComplete, switch Resolve() to a blocking call
	var wg sync.WaitGroup
	if tm.onComplete == nil {
		wg.Add(1)
		tm.onComplete = func(failure error) {
			err = failure
			wg.Done()
		}
	}

	// resolve all the data stores in parallel, and call onComplete when all are done
	go func() {
		failure := tc.resolve()
		if failure != nil {
			err = failure
		}

		for _, dataStore := range tm.dataStores {
			dataStore.Detach()
		}
		tm.dataStores = nil

		if tm.onComplete != nil {
			tm.onComplete(failure)
		}
	}()

	wg.Wait()
	return
}

func (tc *transactionCommit) resolve() error {
	// call all the resolver flush functions in parallel
	var failure error
	var wg sync.WaitGroup
	var mu sync.Mutex
	wg.Add(len(tc.resolvers))
	for _, resolver := range tc.resolvers {
		go func(tr transactionResolver) {
			defer wg.Done()
			err := tr.Flush()
			if err != nil {
				mu.Lock()
				if failure == nil {
					failure = err
				}
				mu.Unlock()
			}
		}(resolver)
	}
	wg.Wait()

	if failure != nil {
		return failure
	}

	// call all the commit functions in parallel
	wg.Add(len(tc.resolvers))
	for _, resolver := range tc.resolvers {
		go func(tr transactionResolver) {
			defer wg.Done()
			err := tr.Commit()
			if err != nil {
				mu.Lock()
				if failure == nil {
					failure = err
				}
				mu.Unlock()
			}
		}(resolver)
	}
	wg.Wait()

	return failure
}

// Add a resolver to the commit that's about to occur
func (tc *transactionCommit) Bind(resolver transactionResolver) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.resolvers = append(tc.resolvers, resolver)
}
