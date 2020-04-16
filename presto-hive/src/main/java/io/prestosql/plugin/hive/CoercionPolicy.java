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
package io.prestosql.plugin.hive;

import java.util.Optional;

public interface CoercionPolicy
{
    boolean canCoerce(HiveType fromType, HiveType toType);

    /**
     * If coercion is not possible due to one or more invalid primitive type change: return an empty Option
     * Otherwise return a composite schema that has:
     * - toType structure (for all nested fields at all depths: map, array, structs) ordering included
     * - fromType primitive types (aka: leaf types)
     *
     * The returned HiveType is aimed to be passed to the file format reader:
     * - so the file reader can read the file with it's correct primitive types
     * - be coercible to the toType thanks to leaf type coercions only
     */
    Optional<HiveType> coercibleIntermediate(HiveType fromType, HiveType toType);
}
