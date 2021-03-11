package dev.psmolinski.kafka.wikipedia;

import lombok.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotEmpty;

@ConfigurationProperties(prefix = "wikipedia", ignoreUnknownFields = false, ignoreInvalidFields = false)
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Validated
public class WikipediaProperties {

    @Builder.Default
    private Topics topics = new Topics();

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Validated
    public static class Topics {

        @NotEmpty
        @Builder.Default
        private String input = "wikipedia.parsed";

        @NotEmpty
        @Builder.Default
        private String output = "wikipedia.parsed.count-by-domain";

    }

}
