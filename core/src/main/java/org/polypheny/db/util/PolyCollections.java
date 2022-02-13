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

package org.polypheny.db.util;

import com.drew.lang.annotations.NotNull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class PolyCollections {

    // todo dl make Immutable
    public static class PolyList<T extends Comparable<?>> extends ArrayList<T> implements Comparable<PolyList<T>>, Collection<T> {


        public PolyList( Collection<T> list ) {
            super( list );
        }


        public PolyList() {
            super();
        }


        /**
         * If the size of left ( this ) is bigger return 1, if smaller return -1,
         * if sum of compared values of left is bigger than right return 1 and vice-versa
         */
        @Override
        public int compareTo( @NotNull PolyList<T> o ) {
            if ( this.size() != o.size() ) {
                return this.size() > o.size() ? 1 : 0;
            }

            long left = 0;
            long right = 0;
            int i = 0;
            int temp;
            for ( T t : this ) {
                temp = ((Comparable) t).compareTo( o.get( i ) );
                if ( temp < 0 ) {
                    right++;
                } else if ( temp > 0 ) {
                    left++;
                }
                i++;
            }

            return left == 0 && right == 0 ? 0 : left > right ? 1 : -1;
        }


        public static <T extends Comparable<T>> PolyList<T> of( Collection<T> list ) {
            return new PolyList<>( list );
        }


    }


    // todo dl make Immutable
    public static class PolyMap<K extends Comparable<?>, V extends Comparable<?>> extends HashMap<K, V> implements Comparable<PolyMap<K, V>> {


        public PolyMap( Map<K, V> map ) {
            super( map );
        }


        public PolyMap() {
            super();
        }


        public static <K extends Comparable<K>, V extends Comparable<V>> PolyMap<K, V> of( Map<K, V> map ) {
            return new PolyMap<>( map );
        }


        @Override
        public int compareTo( @NotNull PolyCollections.PolyMap<K, V> o ) {
            if ( this.size() != o.size() ) {
                return this.size() > o.size() ? 1 : 0;
            }

            int temp;
            for ( Entry<K, V> entry : this.entrySet() ) {
                if ( o.containsKey( entry.getKey() ) ) {
                    temp = ((Comparable) entry.getValue()).compareTo( o.get( entry.getKey() ) );

                    if ( temp != 0 ) {
                        return temp;
                    }
                } else {
                    return -1;
                }
            }
            return 0;
        }

    }

}