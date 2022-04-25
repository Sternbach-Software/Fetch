package flowablefetch

import androidx.room.*
import com.tonyodev.fetch2.Status
import com.tonyodev.fetch2.database.DownloadDao
import com.tonyodev.fetch2.database.DownloadDatabase.Companion.COLUMN_CREATED
import com.tonyodev.fetch2.database.DownloadDatabase.Companion.COLUMN_FILE
import com.tonyodev.fetch2.database.DownloadDatabase.Companion.COLUMN_GROUP
import com.tonyodev.fetch2.database.DownloadDatabase.Companion.COLUMN_ID
import com.tonyodev.fetch2.database.DownloadDatabase.Companion.COLUMN_IDENTIFIER
import com.tonyodev.fetch2.database.DownloadDatabase.Companion.COLUMN_PRIORITY
import com.tonyodev.fetch2.database.DownloadDatabase.Companion.COLUMN_STATUS
import com.tonyodev.fetch2.database.DownloadDatabase.Companion.COLUMN_TAG
import com.tonyodev.fetch2.database.DownloadDatabase.Companion.TABLE_NAME
import com.tonyodev.fetch2.database.DownloadInfo
import kotlinx.coroutines.flow.Flow


@Dao
interface FlowableDownloadDao: DownloadDao {

    @Query("SELECT * FROM $TABLE_NAME")
    fun getFlow(): Flow<List<DownloadInfo>>
}
