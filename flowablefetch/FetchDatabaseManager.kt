package flowablefetch

import com.tonyodev.fetch2.PrioritySort
import com.tonyodev.fetch2.Status
import com.tonyodev.fetch2.database.DownloadInfo
import com.tonyodev.fetch2.database.FetchDatabaseManager
import com.tonyodev.fetch2core.Extras
import com.tonyodev.fetch2core.Logger
import kotlinx.coroutines.flow.Flow
import java.io.Closeable

/**
 * This interface can be implemented by a class to create a custom FetchDatabaseManager.
 * The default Fetch Database Manager is FetchDatabaseManagerImpl which uses sqlite/room
 * to store download information. All methods and fields will be called on Fetch's background thread.
 * */
interface FlowableFetchDatabaseManager<T: DownloadInfo> : FetchDatabaseManager<T> {
    /**
     * Gets a list of all the downloads in the database.
     * */
    fun getFlow(): Flow<List<T>>
}
