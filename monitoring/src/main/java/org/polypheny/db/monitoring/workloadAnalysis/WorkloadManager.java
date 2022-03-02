/*
 * Copyright 2019-2022 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.monitoring.workloadAnalysis;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WorkloadManager {

    private static WorkloadManager INSTANCE = null;


    public static WorkloadManager getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new WorkloadManager();
        }
        return INSTANCE;
    }


    private final TreeMap<Timestamp, WorkloadInformation> allTimes = new TreeMap<>();

    private final TreeMap<Timestamp, WorkloadInformation> minuteTimeline = new TreeMap<>();


    public void updateWorkloadTimeline( Timestamp timestamp, WorkloadInformation workloadInformation ) {

        LocalDateTime timeWithoutSeconds = timestamp.toLocalDateTime().truncatedTo( ChronoUnit.MINUTES );

        allTimes.put( timestamp, workloadInformation );

        if(minuteTimeline.containsKey( Timestamp.valueOf( timeWithoutSeconds) )){
            WorkloadInformation workloadInfo = minuteTimeline.remove( Timestamp.valueOf( timeWithoutSeconds) );
            workloadInfo.updateInfo( workloadInformation );
            minuteTimeline.put( Timestamp.valueOf( timeWithoutSeconds), workloadInfo  );

        }else{
            minuteTimeline.put(Timestamp.valueOf( timeWithoutSeconds), workloadInformation  );
        }

        List<Timestamp> timesToDelete = minuteTimeline.keySet().stream().filter( time -> time.getTime() > timestamp.getTime() + 7200000).collect( Collectors.toList()  );

        for ( Timestamp deleteTime : timesToDelete ) {
            minuteTimeline.remove( deleteTime );
        }

    }


    public Object getWorkloadInformation() {
        return minuteTimeline;
    }

}