package workpool

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
)

var (
	ErrCapacity = errors.New("Thread Pool At Capacity")
)

type (
	// poolWork is passed into the queue for work to be performed.
	poolWork struct {
		work          PoolWorker // The Work to be performed.
		resultChannel chan error // Used to inform the queue operaion is complete.
	}

	// WorkPool implements a work pool with the specified concurrency level and queue capacity.
	WorkPool struct {
		shutdownQueueChannel chan string     // Channel used to shut down the queue routine.
		shutdownWorkChannel  chan struct{}   // Channel used to shut down the work routines.
		shutdownWaitGroup    sync.WaitGroup  // The WaitGroup for shutting down existing routines.
		queueChannel         chan poolWork   // Channel used to sync access to the queue.
		workChannel          chan PoolWorker // Channel used to process work.
		queuedWork           int32           // The number of work items queued.
		activeRoutines       int32           // The number of routines active.
		queueCapacity        int32           // The max number of items we can store in the queue.
	}
)

// PoolWorker must be implemented by the object we will perform work on, now.
type PoolWorker interface {
	DoWork(workRoutine int)
}

// init is called when the system is inited.
func init() {
	log.SetPrefix("TRACE: ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

// New creates a new WorkPool.
func New(numberOfRoutines int, queueCapacity int32) *WorkPool {
	workPool := WorkPool{
		shutdownQueueChannel: make(chan string),
		shutdownWorkChannel:  make(chan struct{}),
		queueChannel:         make(chan poolWork),
		workChannel:          make(chan PoolWorker, queueCapacity),
		queuedWork:           0,
		activeRoutines:       0,
		queueCapacity:        queueCapacity,
	}

	// Add the total number of routines to the wait group
	workPool.shutdownWaitGroup.Add(numberOfRoutines)

	// Launch the work routines to process work
	for workRoutine := 0; workRoutine < numberOfRoutines; workRoutine++ {
		go workPool.workRoutine(workRoutine)
	}

	// Start the queue routine to capture and provide work
	go workPool.queueRoutine()

	return &workPool
}

// Shutdown will release resources and shutdown all processing.
func (workPool *WorkPool) Shutdown(goRoutine string) (err error) {
	defer catchPanic(&err, goRoutine, "Shutdown")

	writeStdout(goRoutine, "Shutdown", "Started")
	writeStdout(goRoutine, "Shutdown", "Queue Routine")

	workPool.shutdownQueueChannel <- "Down"
	<-workPool.shutdownQueueChannel

	close(workPool.queueChannel)
	close(workPool.shutdownQueueChannel)

	writeStdout(goRoutine, "Shutdown", "Shutting Down Work Routines")

	// Close the channel to shut things down.
	close(workPool.shutdownWorkChannel)
	workPool.shutdownWaitGroup.Wait()

	close(workPool.workChannel)

	writeStdout(goRoutine, "Shutdown", "Completed")
	return err
}

// PostWork will post work into the WorkPool. This call will block until the Queue routine reports back
// success or failure that the work is in queue.
func (workPool *WorkPool) PostWork(goRoutine string, work PoolWorker) (err error) {
	defer catchPanic(&err, goRoutine, "PostWork")

	poolWork := poolWork{work, make(chan error)}

	defer close(poolWork.resultChannel)

	workPool.queueChannel <- poolWork
	err = <-poolWork.resultChannel

	return err
}

// QueuedWork will return the number of work items in queue.
func (workPool *WorkPool) QueuedWork() int32 {
	return atomic.AddInt32(&workPool.queuedWork, 0)
}

// ActiveRoutines will return the number of routines performing work.
func (workPool *WorkPool) ActiveRoutines() int32 {
	return atomic.AddInt32(&workPool.activeRoutines, 0)
}

// CatchPanic is used to catch any Panic and log exceptions to Stdout. It will also write the stack trace.
func catchPanic(err *error, goRoutine string, functionName string) {
	if r := recover(); r != nil {
		// Capture the stack trace
		buf := make([]byte, 10000)
		runtime.Stack(buf, false)

		writeStdoutf(goRoutine, functionName, "PANIC Defered [%v] : Stack Trace : %v", r, string(buf))

		if err != nil {
			*err = fmt.Errorf("%v", r)
		}
	}
}

// writeStdout is used to write a system message directly to stdout.
func writeStdout(goRoutine string, functionName string, message string) {
	log.Printf("%s : %s : %s\n", goRoutine, functionName, message)
}

// writeStdoutf is used to write a formatted system message directly stdout.
func writeStdoutf(goRoutine string, functionName string, format string, a ...interface{}) {
	writeStdout(goRoutine, functionName, fmt.Sprintf(format, a...))
}

// workRoutine performs the work required by the work pool
func (workPool *WorkPool) workRoutine(workRoutine int) {
	for {
		select {
		// Shutdown the WorkRoutine.
		case <-workPool.shutdownWorkChannel:
			writeStdout(fmt.Sprintf("WorkRoutine %d", workRoutine), "workRoutine", "Going Down")
			workPool.shutdownWaitGroup.Done()
			return

		// There is work in the queue.
		case poolWorker := <-workPool.workChannel:
			workPool.safelyDoWork(workRoutine, poolWorker)
			break
		}
	}
}

// safelyDoWork executes the user DoWork method.
func (workPool *WorkPool) safelyDoWork(workRoutine int, poolWorker PoolWorker) {
	defer catchPanic(nil, "WorkRoutine", "SafelyDoWork")
	defer atomic.AddInt32(&workPool.activeRoutines, -1)

	// Update the counts
	atomic.AddInt32(&workPool.queuedWork, -1)
	atomic.AddInt32(&workPool.activeRoutines, 1)

	// Perform the work
	poolWorker.DoWork(workRoutine)
}

// queueRoutine captures and provides work.
func (workPool *WorkPool) queueRoutine() {
	for {
		select {
		// Shutdown the QueueRoutine.
		case <-workPool.shutdownQueueChannel:
			writeStdout("Queue", "queueRoutine", "Going Down")
			workPool.shutdownQueueChannel <- "Down"
			return

		// Post work to be processed.
		case queueItem := <-workPool.queueChannel:
			// If the queue is at capacity don't add it.
			if atomic.AddInt32(&workPool.queuedWork, 0) == workPool.queueCapacity {
				queueItem.resultChannel <- ErrCapacity
				continue
			}

			// Increment the queued work count.
			atomic.AddInt32(&workPool.queuedWork, 1)

			// Queue the work for the WorkRoutine to process.
			workPool.workChannel <- queueItem.work

			// Tell the caller the work is queued.
			queueItem.resultChannel <- nil
			break
		}
	}
}

