/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.command.log.internal;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.cli.Option.builder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.agrona.LangUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.reaktivity.command.log.internal.layouts.StreamsLayout;
import org.reaktivity.nukleus.Configuration;

public final class LogCommand
{
    private static final Pattern SENDER_NAME = Pattern.compile("([^#]+).*");

    private static final long MAX_PARK_NS = MILLISECONDS.toNanos(100L);
    private static final long MIN_PARK_NS = MILLISECONDS.toNanos(1L);
    private static final int MAX_YIELDS = 30;
    private static final int MAX_SPINS = 20;

    private final Path directory;
    private final boolean verbose;
    private final long streamsCapacity;
    private final long throttleCapacity;

    private LogCommand(
        Configuration config,
        boolean verbose)
    {
        this.directory = config.directory();
        this.verbose = verbose;
        this.streamsCapacity = config.streamsBufferCapacity();
        this.throttleCapacity = config.throttleBufferCapacity();
    }

    private boolean isStreamsFile(
        Path path)
    {
        return path.getNameCount() - directory.getNameCount() == 3 &&
               "streams".equals(path.getName(path.getNameCount() - 2).toString()) &&
               Files.isRegularFile(path);
    }

    private Loggable newLoggable(
        Path path)
    {
        StreamsLayout layout = new StreamsLayout.Builder()
                .path(path)
                .streamsCapacity(streamsCapacity)
                .throttleCapacity(throttleCapacity)
                .readonly(true)
                .build();

        String receiver = path.getName(path.getNameCount() - 3).toString();
        String sender = sender(path);

        return new Loggable(receiver, sender, layout, verbose);
    }

    private void onDiscovered(
        Path path)
    {
        if (verbose)
        {
            System.out.println("Discovered: " + path);
        }
    }

    private void invoke()
    {
        try (Stream<Path> files = Files.walk(directory, 3))
        {
            Loggable[] loggables = files.filter(this::isStreamsFile)
                 .peek(this::onDiscovered)
                 .map(this::newLoggable)
                 .collect(toList())
                 .toArray(new Loggable[0]);

            final IdleStrategy idleStrategy = new BackoffIdleStrategy(MAX_SPINS, MAX_YIELDS, MIN_PARK_NS, MAX_PARK_NS);

            while (true)
            {
                int workCount = 0;

                for (int i=0; i < loggables.length; i++)
                {
                    workCount += loggables[i].process();
                }

                idleStrategy.idle(workCount);
            }
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private static String sender(
        Path path)
    {
        Matcher matcher = SENDER_NAME.matcher(path.getName(path.getNameCount() - 1).toString());
        if (matcher.matches())
        {
            return matcher.group(1);
        }
        else
        {
            throw new IllegalStateException();
        }
    }

    public static void main(String[] args) throws Exception
    {
        CommandLineParser parser = new DefaultParser();

        Options options = new Options();
        options.addOption(builder("h").longOpt("help").desc("print this message").build());
        options.addOption(builder("d").longOpt("directory").hasArg().desc("configuration directory").build());
        options.addOption(builder("v").longOpt("verbose").desc("verbose output").build());
        options.addOption(builder("D").hasArgs().desc("define property").build());

        CommandLine cmdline = parser.parse(options, args);

        if (cmdline.hasOption("help") || !cmdline.hasOption("directory"))
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("log", options);
        }
        else
        {
            String directory = cmdline.getOptionValue("directory");
            boolean verbose = cmdline.hasOption("verbose");
            String[] defines = cmdline.getOptionValues('D');

            Properties properties = new Properties();
            setDefinedProperties(properties, defines);
            properties.setProperty(Configuration.DIRECTORY_PROPERTY_NAME, directory);

            final Configuration config = new Configuration(properties);

            LogCommand command = new LogCommand(config, verbose);
            command.invoke();
        }
    }

    private static void setDefinedProperties(
        Properties properties,
        String[] defines)
    {
        if (defines != null)
        {
            for (int i=0; i < defines.length; i += 2)
            {
                String name = defines[i];
                String value = defines[i+1];
                properties.setProperty(name, value);
            }
        }
    }
}
