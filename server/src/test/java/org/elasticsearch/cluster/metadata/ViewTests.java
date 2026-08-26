/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.ViewTestsUtils.randomName;
import static org.elasticsearch.cluster.metadata.ViewTestsUtils.randomView;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ViewTests extends AbstractXContentSerializingTestCase<View> {

    @Override
    protected View doParseInstance(XContentParser parser) throws IOException {
        return View.fromXContent(parser);
    }

    @Override
    protected View createTestInstance() {
        return randomView(randomName());
    }

    @Override
    protected View mutateInstance(View instance) {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> new View(
                instance.name(),
                instance.query() + "-mutated",
                instance.description(),
                instance.managed(),
                instance.metadata()
            );
            case 1 -> new View(
                instance.name(),
                instance.query(),
                randomValueOtherThan(instance.description(), () -> randomBoolean() ? randomAlphaOfLengthBetween(1, 20) : null),
                instance.managed(),
                instance.metadata()
            );
            case 2 -> new View(instance.name(), instance.query(), instance.description(), instance.managed() == false, instance.metadata());
            case 3 -> new View(
                instance.name(),
                instance.query(),
                instance.description(),
                instance.managed(),
                randomValueOtherThan(instance.metadata(), () -> randomBoolean() ? Map.of("k", randomAlphaOfLength(6)) : null)
            );
            default -> throw new AssertionError("unknown mutation branch");
        };
    }

    @Override
    protected Writeable.Reader<View> instanceReader() {
        return View::new;
    }

    @Override
    protected void assertEqualInstances(View expectedInstance, View newInstance) {
        assertNotSame(expectedInstance, newInstance);
        assertEqualViews(expectedInstance, newInstance);
    }

    public static void assertEqualViews(View expectedInstance, View newInstance) {
        assertThat(newInstance, equalTo(expectedInstance));
        assertThat(newInstance.isHidden(), equalTo(expectedInstance.managed()));
        assertFalse(newInstance.isSystem());
    }

    public void testManagedViewIsHidden() {
        View managed = new View("sys", "FROM logs", "owned", true, Map.of("managed_by", "kibana"));
        assertTrue(managed.managed());
        assertTrue(managed.isHidden());
        assertFalse(managed.isSystem());
        assertThat(managed.description(), equalTo("owned"));
        assertThat(managed.metadata(), equalTo(Map.of("managed_by", "kibana")));
    }

    public void testUnmanagedViewIsNotHidden() {
        View view = new View("user", "FROM logs");
        assertFalse(view.managed());
        assertFalse(view.isHidden());
        assertThat(view.description(), nullValue());
        assertThat(view.metadata(), nullValue());
    }

    public void testRejectsOversizedDescription() {
        String tooLong = "x".repeat(View.MAX_DESCRIPTION_LENGTH + 1);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new View("v", "FROM x", tooLong, false, null));
        assertThat(
            e.getMessage(),
            equalTo(
                "view [description] is too large: "
                    + tooLong.length()
                    + " characters, the maximum allowed is "
                    + View.MAX_DESCRIPTION_LENGTH
            )
        );
    }

    public void testRejectsOversizedMeta() {
        String big = "x".repeat(View.MAX_META_BYTES);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new View("v", "FROM x", null, false, Map.of("blob", big))
        );
        assertThat(e.getMessage(), containsString("view [_meta] is too large"));
    }

    public void testBwcSerializationOmitsNewFields() throws IOException {
        View view = new View("alerts", "FROM .rule-events", "Latest alerts", true, Map.of("managed_by", "alerting_v2"));
        View copied = copyWriteable(view, writableRegistry(), View::new, TransportVersion.fromName("esql_views"));
        assertThat(copied.name(), equalTo("alerts"));
        assertThat(copied.query(), equalTo("FROM .rule-events"));
        assertThat(copied.description(), nullValue());
        assertFalse(copied.managed());
        assertThat(copied.metadata(), nullValue());
        assertFalse(copied.isHidden());
    }

    public void testXContentOmitsDefaultMetadata() throws IOException {
        View view = new View("dogs", "FROM test");
        String json = view.toString();
        assertFalse(json.contains("description"));
        assertThat(json, containsString("\"managed\":false"));
        assertFalse(json.contains("_meta"));
    }
}
