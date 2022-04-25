package flowablefetch

import com.tonyodev.fetch2.*
import com.tonyodev.fetch2.database.DownloadInfo
import com.tonyodev.fetch2.fetch.FetchHandler
import com.tonyodev.fetch2core.*
import kotlinx.coroutines.flow.Flow
import java.io.Closeable

/**
 * This handlerWrapper class handles all tasks and operations of Fetch.
 * */
interface FlowableFetchHandler : FetchHandler {
    fun getDownloadsFlow(): Flow<List<Download>>
}
