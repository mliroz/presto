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
package io.prestosql.plugin.session;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.session.SessionConfigurationContext;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SessionMatchSpec
{
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> sourceRegex;
    private final Set<String> clientTags;
    private final Optional<String> queryType;
    private final Optional<Pattern> resourceGroupRegex;
    private final SessionProperties sessionProperties;

    public static class SessionProperties
    {
        static final SessionProperties empty = new SessionProperties(ImmutableMap.of(), ImmutableMap.of());
        private final Map<String, String> generalProperties;
        private final Map<String, Map<String, String>> catalogsProperties;

        public SessionProperties(
                Map<String, String> generalProperties,
                Map<String, Map<String, String>> catalogsProperties)
        {
            this.generalProperties = generalProperties;
            this.catalogsProperties = catalogsProperties;
        }

        public Map<String, String> getGeneralProperties()
        {
            return generalProperties;
        }

        public Map<String, Map<String, String>> getCatalogsProperties()
        {
            return catalogsProperties;
        }
    }

    @JsonCreator
    public SessionMatchSpec(
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("source") Optional<Pattern> sourceRegex,
            @JsonProperty("clientTags") Optional<List<String>> clientTags,
            @JsonProperty("queryType") Optional<String> queryType,
            @JsonProperty("group") Optional<Pattern> resourceGroupRegex,
            @JsonProperty("sessionProperties") Map<String, String> sessionProperties,
            @JsonProperty("catalogSessionProperties") Map<String, Map<String, String>> catalogSessionProperties)
    {
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.sourceRegex = requireNonNull(sourceRegex, "sourceRegex is null");
        requireNonNull(clientTags, "clientTags is null");
        this.clientTags = ImmutableSet.copyOf(clientTags.orElse(ImmutableList.of()));
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.resourceGroupRegex = requireNonNull(resourceGroupRegex, "resourceGroupRegex is null");
        if (sessionProperties == null && catalogSessionProperties == null) {
            throw new NullPointerException("sessionProperties and catalogSessionProperties are null");
        }
        this.sessionProperties = new SessionProperties(
                (sessionProperties == null) ? ImmutableMap.of() : ImmutableMap.copyOf(sessionProperties),
                (catalogSessionProperties == null) ? ImmutableMap.of() : ImmutableMap.copyOf(catalogSessionProperties));
    }

    public SessionProperties match(SessionConfigurationContext context)
    {
        if (userRegex.isPresent() && !userRegex.get().matcher(context.getUser()).matches()) {
            return SessionProperties.empty;
        }
        if (sourceRegex.isPresent()) {
            String source = context.getSource().orElse("");
            if (!sourceRegex.get().matcher(source).matches()) {
                return SessionProperties.empty;
            }
        }
        if (!clientTags.isEmpty() && !context.getClientTags().containsAll(clientTags)) {
            return SessionProperties.empty;
        }

        if (queryType.isPresent()) {
            String contextQueryType = context.getQueryType().orElse("");
            if (!queryType.get().equalsIgnoreCase(contextQueryType)) {
                return SessionProperties.empty;
            }
        }

        if (resourceGroupRegex.isPresent() && !resourceGroupRegex.get().matcher(context.getResourceGroupId().toString()).matches()) {
            return SessionProperties.empty;
        }

        return sessionProperties;
    }

    @JsonProperty("user")
    public Optional<Pattern> getUserRegex()
    {
        return userRegex;
    }

    @JsonProperty("source")
    public Optional<Pattern> getSourceRegex()
    {
        return sourceRegex;
    }

    @JsonProperty
    public Set<String> getClientTags()
    {
        return clientTags;
    }

    @JsonProperty
    public Optional<String> getQueryType()
    {
        return queryType;
    }

    @JsonProperty("group")
    public Optional<Pattern> getResourceGroupRegex()
    {
        return resourceGroupRegex;
    }

    @JsonIgnore
    public SessionProperties getAllSessionProperties()
    {
        return sessionProperties;
    }

    @JsonProperty("sessionProperties")
    public Map<String, String> getSessionProperties()
    {
        return sessionProperties.generalProperties;
    }

    @JsonProperty("catalogSessionProperties")
    public Map<String, Map<String, String>> getCatalogSessionProperties()
    {
        return sessionProperties.catalogsProperties;
    }

    public static class Mapper
            implements RowMapper<SessionMatchSpec>
    {
        @Override
        public SessionMatchSpec map(ResultSet resultSet, StatementContext context)
                throws SQLException
        {
            Map<String, String> sessionProperties = getProperties(
                    Optional.ofNullable(resultSet.getString("session_property_names")),
                    Optional.ofNullable(resultSet.getString("session_property_values")));

            return new SessionMatchSpec(
                    Optional.ofNullable(resultSet.getString("user_regex")).map(Pattern::compile),
                    Optional.ofNullable(resultSet.getString("source_regex")).map(Pattern::compile),
                    Optional.ofNullable(resultSet.getString("client_tags")).map(tag -> Splitter.on(",").splitToList(tag)),
                    Optional.ofNullable(resultSet.getString("query_type")),
                    Optional.ofNullable(resultSet.getString("group_regex")).map(Pattern::compile),
                    sessionProperties,
                    new HashMap<>());
        }

        private static Map<String, String> getProperties(Optional<String> names, Optional<String> values)
        {
            if (!names.isPresent()) {
                return ImmutableMap.of();
            }

            checkArgument(values.isPresent(), "names are present, but values are not");
            List<String> sessionPropertyNames = Splitter.on(",").splitToList(names.get());
            List<String> sessionPropertyValues = Splitter.on(",").splitToList(values.get());
            checkArgument(sessionPropertyNames.size() == sessionPropertyValues.size(),
                    "The number of property names and values should be the same");

            Map<String, String> sessionProperties = new HashMap<>();
            for (int i = 0; i < sessionPropertyNames.size(); i++) {
                sessionProperties.put(sessionPropertyNames.get(i), sessionPropertyValues.get(i));
            }

            return sessionProperties;
        }
    }
}
