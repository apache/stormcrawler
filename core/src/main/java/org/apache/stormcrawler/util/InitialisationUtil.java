/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.stormcrawler.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public final class InitialisationUtil {

    /**
     * Initializes a class from {@code qualifiedClassName} as type {@code superClass}. Further
     * constrains for implemented classes and interfaces are possible via {@code
     * furtherSuperClasses}.
     *
     * <p>Requirements:
     *
     * <p>{@code qualifiedClassName}:
     *
     * <ol>
     *   <li>Has an accessible empty constructor.
     *   <li>Points to a class extending/implementing {@code superClass}.
     *   <li>Points to a class extending/implementing all {@code furtherSuperClasses}.
     *   <li>Points to a class that is not primitive, abstract or an interface.
     *   <li>Points to an existing class.
     *   <li>Is not blank.
     * </ol>
     *
     * <p>{@code superClass}:
     *
     * <ol>
     *   <li>Does not point to a primitive.
     *   <li>Points to a class extending/implemented by the class of {@code qualifiedClassName}.
     * </ol>
     *
     * <p>{@code furtherSuperClasses}
     *
     * <ol>
     *   <li>Points to classes extending/implemented by the class of {@code qualifiedClassName}.
     * </ol>
     *
     * @param qualifiedClassName the qualified name for the class to be initialized.
     * @param superClass defines the type to be instantiated
     * @param furtherSuperClasses further checks for specific interfaces etc.
     * @param <T> the type of the returned class.
     * @return an instance of {@code qualifiedClassName} of type {@code superClass}
     *     extending/implementing all {@code furtherSuperClasses}.
     * @throws RuntimeException if the {@code superClass} is not assignable to the class of {@code
     *     clazz}
     * @throws RuntimeException if any of the {@code furtherSuperClasses} is not assignable to the
     *     class of {@code clazz}
     * @throws RuntimeException if the {@code superClass} is a primitive
     * @throws RuntimeException if the {@code clazz} is not pointing to a class.
     * @throws RuntimeException if the {@code clazz} is blank.
     * @throws RuntimeException if either the underlying constructor is inaccessbible, no empty
     *     constructor found or the underlying class is somehow abstract.
     * @throws RuntimeException if the underlying constructor threw an exception.
     * @throws RuntimeException if the {@code clazz} points to a interface, abstract class or
     *     primitive.
     */
    @NotNull
    @Contract(pure = true)
    public static <T> T initializeFromQualifiedName(
            @NotNull String qualifiedClassName,
            @NotNull Class<? extends T> superClass,
            @NotNull Class<?>... furtherSuperClasses) {
        return initializeFromClassUnchecked(
                getClassFor(qualifiedClassName, superClass, furtherSuperClasses));
    }

    /**
     * Retrieves a class-instance for {@code qualifiedClassName} extending {@code T}. {@code T} is
     * supplied by constraining the type of {@code qualifiedClassName} to {@code superClass}.
     * Further constrains for implemented classes and interfaces are possible via {@code
     * furtherSuperClasses}.
     *
     * <p>Requirements:
     *
     * <p>{@code clazz}:
     *
     * <ol>
     *   <li>Points to a class extending/implementing {@code superClass}.
     *   <li>Points to a class extending/implementing all {@code furtherSuperClasses}.
     *   <li>Points to a class that is not primitive, abstract or an interface.
     *   <li>Points to an existing class.
     *   <li>Is not blank.
     * </ol>
     *
     * <p>{@code superClass}:
     *
     * <ol>
     *   <li>Does not point to a primitive.
     *   <li>Points to a class extending/implemented by the class of {@code clazz}.
     * </ol>
     *
     * <p>{@code furtherSuperClasses}
     *
     * <ol>
     *   <li>Points to classes extending/implemented by the class of {@code clazz}.
     * </ol>
     *
     * @param qualifiedClassName the qualified name for the class to be retrieved.
     * @param superClass defines the type to be instantiated
     * @param furtherSuperClasses further checks for specific interfaces etc.
     * @param <T> the type extended by the returned class.
     * @return an instance of {@code clazz} of type {@code superClass} extending/implementing all
     *     {@code furtherSuperClasses}.
     * @throws RuntimeException if the {@code superClass} is not assignable to the class of {@code
     *     clazz}
     * @throws RuntimeException if any of the {@code furtherSuperClasses} is not assignable to the
     *     class of {@code clazz}
     * @throws RuntimeException if the {@code superClass} is a primitive
     * @throws RuntimeException if the {@code clazz} is not pointing to a class.
     * @throws RuntimeException if the {@code clazz} is blank.
     */
    @NotNull
    @Contract(pure = true)
    public static <T> Class<? extends T> getClassFor(
            @NotNull String qualifiedClassName,
            @NotNull Class<? extends T> superClass,
            @NotNull Class<?>... furtherSuperClasses) {
        if (StringUtils.isBlank(qualifiedClassName)) {
            throw new RuntimeException("The qualified class name is empty!");
        }

        checkSuperClass(superClass);

        try {
            Class<?> clazz = Class.forName(qualifiedClassName);
            return requireSuperClass(clazz, superClass, furtherSuperClasses);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("The class " + qualifiedClassName + " was not found!", e);
        }
    }

    /**
     * Initializes a class from {@code clazz} as type {@code superClass}. Further constrains for
     * implemented classes and interfaces are possible via {@code furtherSuperClasses}.
     *
     * <p>Requirements:
     *
     * <p>{@code clazz}:
     *
     * <ol>
     *   <li>Has an accessible empty constructor.
     *   <li>Points to a class extending/implementing {@code superClass}.
     *   <li>Points to a class extending/implementing all {@code furtherSuperClasses}.
     *   <li>Points to a class that is not primitive, abstract or an interface.
     *   <li>Points to an existing class.
     *   <li>Is not blank.
     * </ol>
     *
     * <p>{@code superClass}:
     *
     * <ol>
     *   <li>Does not point to a primitive.
     *   <li>Points to a class extending/implemented by the class of {@code clazz}.
     * </ol>
     *
     * <p>{@code furtherSuperClasses}
     *
     * <ol>
     *   <li>Points to classes extending/implemented by the class of {@code clazz}.
     * </ol>
     *
     * @param clazz the class to be initialized.
     * @param superClass defines the type to be instantiated
     * @param furtherSuperClasses further checks for specific interfaces etc.
     * @param <T> the type of the returned class.
     * @return an instance of {@code clazz} of type {@code superClass} extending/implementing all
     *     {@code furtherSuperClasses}.
     * @throws RuntimeException if either the underlying constructor is inaccessbible, no empty
     *     constructor found or the underlying class is somehow abstract.
     * @throws RuntimeException if the underlying constructor threw an exception.
     * @throws RuntimeException if the {@code clazz} points to a interface, abstract class or
     *     primitive.
     * @throws RuntimeException if the {@code superClass} is not assignable to the class of {@code
     *     clazz}
     * @throws RuntimeException if any of the {@code furtherSuperClasses} is not assignable to the
     *     class of {@code clazz}
     * @throws RuntimeException if the {@code superClass} is a primitive
     */
    @NotNull
    @Contract(pure = true)
    public static <T> T initializeFromClass(
            @NotNull Class<?> clazz,
            @NotNull Class<? extends T> superClass,
            @NotNull Class<?>... furtherSuperClasses) {
        return initializeFromClassUnchecked(
                requireSuperClass(clazz, superClass, furtherSuperClasses));
    }

    /**
     * Initializes a class from {@code clazz} of type {@code T}.
     *
     * <p>Requirements:
     *
     * <p>{@code clazz}:
     *
     * <ol>
     *   <li>Points to a class that is not primitive, abstract or an interface.
     * </ol>
     *
     * @param clazz the class to be initialized.
     * @param <T> the type of the returned class.
     * @return an instance of {@code clazz}.
     * @throws RuntimeException if either the underlying constructor is inaccessbible, no empty
     *     constructor found or the underlying class is somehow abstract.
     * @throws RuntimeException if the underlying constructor threw an exception.
     * @throws RuntimeException if the {@code clazz} points to a interface, abstract class or
     *     primitive.
     */
    @NotNull
    @Contract(pure = true)
    public static <T> T initializeFromClass(@NotNull Class<? extends T> clazz) {
        checkClazzSimple(clazz);
        return initializeFromClassUnchecked(clazz);
    }

    /*
     * The unchecked method to initialize a clazz
     */
    @NotNull
    @Contract(pure = true)
    private static <T> T initializeFromClassUnchecked(@NotNull Class<? extends T> clazz) {
        try {
            final Constructor<? extends T> declaredConstructor = clazz.getDeclaredConstructor();
            return declaredConstructor.newInstance();
        } catch (InvocationTargetException e) {
            throw new RuntimeException("The underlying constructor threw an exception.", e);
        } catch (InstantiationException e) {
            throw new RuntimeException("The underlying class is abstract.", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(
                    "The underlying object enforces java language access control and underlying constructor in inaccessible.",
                    e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(
                    "There was no empty constructor found for " + clazz.getName() + ".", e);
        }
    }

    /**
     * Asserts the following:
     *
     * <p>{@code clazz}:
     *
     * <ol>
     *   <li>Points to a class extending/implementing {@code superClass}.
     *   <li>Points to a class extending/implementing all {@code furtherSuperClasses}.
     *   <li>Points to a class that is not primitive, abstract or an interface.
     * </ol>
     *
     * <p>{@code superClass}:
     *
     * <ol>
     *   <li>Does not point to a primitive.
     *   <li>Points to a class extending/implemented by the class of {@code clazz}.
     * </ol>
     *
     * <p>{@code furtherSuperClasses}
     *
     * <ol>
     *   <li>Points to classes extending/implemented by the class of {@code clazz}.
     * </ol>
     *
     * @return Either fails or returns {@code clazz} cast to Class&lt;? extends T&gt;
     * @throws RuntimeException if the {@code clazz} points to a interface, abstract class or
     *     primitive.
     * @throws RuntimeException if the {@code superClass} is not assignable to the class of {@code
     *     clazz}
     * @throws RuntimeException if any of the {@code furtherSuperClasses} is not assignable to the
     *     class of {@code clazz}
     * @throws RuntimeException if the {@code superClass} is a primitive
     */
    @SuppressWarnings("unchecked")
    @Contract(pure = true)
    public static <T> Class<? extends T> requireSuperClass(
            @NotNull Class<?> clazz,
            @NotNull Class<? extends T> superClass,
            @NotNull Class<?>... furtherSuperClasses) {
        checkSuperClass(superClass);

        checkClazzSimple(clazz);

        if (!superClass.isAssignableFrom(clazz)) {
            throw new RuntimeException(
                    buildNotAssignableExceptionText(clazz.getName(), superClass));
        }

        for (Class<?> assignable : furtherSuperClasses) {
            if (!assignable.isAssignableFrom(clazz)) {
                throw new RuntimeException(
                        buildNotAssignableExceptionText(clazz.getName(), superClass));
            }
        }

        return (Class<? extends T>) clazz;
    }

    /**
     * Asserts the following:
     *
     * <p>{@code superClass}:
     *
     * <ol>
     *   <li>Does not point to a primitive.
     * </ol>
     *
     * @throws RuntimeException if the {@code superClass} is a primitive
     */
    private static void checkSuperClass(@NotNull Class<?> superClass) {
        if (superClass.isPrimitive()) {
            throw new RuntimeException(
                    "The superClass" + superClass.getName() + " is an primitive!");
        }
    }

    /**
     * Asserts the following:
     *
     * <p>{@code clazz}:
     *
     * <ol>
     *   <li>Points to a class that is not primitive, abstract or an interface.
     * </ol>
     *
     * @throws RuntimeException if the {@code clazz} points to a interface, abstract class or
     *     primitive.
     */
    @Contract(pure = true)
    private static void checkClazzSimple(@NotNull Class<?> clazz) {
        if (clazz.isInterface()) {
            throw new RuntimeException(clazz.getName() + " is an interface!");
        } else if (Modifier.isAbstract(clazz.getModifiers())) {
            throw new RuntimeException(clazz.getName() + " is an abstract class!");
        } else if (clazz.isPrimitive()) {
            throw new RuntimeException(clazz.getName() + " is an primitive!");
        }
    }

    private static String buildNotAssignableExceptionText(
            @NotNull String qualifiedClassName, @NotNull Class<?> assignable) {
        StringBuilder sb = new StringBuilder();
        sb.append("Class ").append(qualifiedClassName).append(" must ");
        String classOrInterface;
        String implementOrExtend;
        if (assignable.isInterface()) {
            classOrInterface = "interface";
            implementOrExtend = "implement";
        } else if (Modifier.isAbstract(assignable.getModifiers())) {
            classOrInterface = "abstract class";
            implementOrExtend = "extend";
        } else {
            classOrInterface = "class";
            implementOrExtend = "extend";
        }
        sb.append(implementOrExtend).append(" the ").append(classOrInterface);
        return sb.toString();
    }

    private InitialisationUtil() {}
}
