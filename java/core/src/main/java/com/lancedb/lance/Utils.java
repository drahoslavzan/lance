/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lancedb.lance;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/** Utility. */
public class Utils {

  /** Convert Arrow Schema to FFI Arrow Schema. */
  public static ArrowSchema toFfi(Schema schema, BufferAllocator allocator) {
    var arrowSchema = ArrowSchema.allocateNew(allocator);
    Data.exportSchema(allocator, schema, null, arrowSchema);
    return arrowSchema;
  }

  /** Convert optional array to optional list for JNI processing. */
  public static Optional<List<String>> convert(Optional<String[]> optionalArray) {
    if (optionalArray.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(Arrays.asList(optionalArray.get()));
  }
}
