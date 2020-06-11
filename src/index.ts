import {
    BlankNode,
    DefaultDataFactory, isLiteral, isLiteralLike, isNamedNodeLike,
    isQuad, isQuadGraph,
    isQuadLike, isQuadPredicate, isQuadSubject, Literal, LiteralLike, NamedNode,
    Quad, QuadGraphLike, QuadPredicateLike,
    QuadSubjectLike
} from "@opennetwork/rdf-data-model"
import { isAsyncIterable, isIterable, isPromise } from "iterable"
import { encode } from "@opennetwork/rdf-namespace-json"
import * as ns from "./namespace"

export * from "./namespace"

export interface ThingFn {
    (knownAs: NamedNode | BlankNode, options: TransformOptions): AsyncGenerator<Quad>
}

export interface TransformFn<LiteralType = unknown, BinaryType = unknown, ContextType= unknown> {
    (source: TransformableSource, options: TransformOptions<LiteralType, BinaryType, ContextType>): void | AsyncGenerator<Quad>
}

export interface TransformOptions<LiteralType = unknown, BinaryType = unknown, ContextType = unknown> {
    context?: ContextType
    literalQuad: {
        subject: QuadSubjectLike
        predicate?: QuadPredicateLike
        graph?: QuadGraphLike
    },
    profileQuad?: boolean | {
        predicate?: QuadPredicateLike
        graph?: QuadGraphLike
    }
    isBinaryType?(source: unknown): source is BinaryType
    getHex?(source: BinaryType): undefined | Promise<undefined> | string | Promise<string>
    getBase64?(source: BinaryType): undefined | Promise<undefined> |string | Promise<string>
    isLiteralType?(source: unknown): source is LiteralType
    getLiteral(source: LiteralType): LiteralLike | Promise<LiteralLike>
    isUnknown?(source: unknown): boolean
    onUnknown?(source: TransformableSource): TransformableSource
    getQuad?: TransformFn
    transformations?: TransformFn[]
}

export type TransformableAsyncIterableSource = AsyncIterable<TransformableSource>
export type TransformableIterableSource = Iterable<TransformableSource>
export type TransformablePromiseSource = Promise<TransformableSource>
export type TransformableFunctionSource = ThingFn | ((knownAs: NamedNode | BlankNode, options: TransformOptions) => TransformableSource)
export type TransformableSource =
    | TransformableAsyncIterableSource
    | TransformableIterableSource
    | TransformablePromiseSource
    | TransformableFunctionSource
    // Above is more documenting what we _expect_ but in the end we do not need to know the type of the source
    | unknown

export async function *transform<LiteralType = unknown, BinaryType = unknown, ContextType = unknown>(source: TransformableSource, options: TransformOptions<BinaryType, LiteralType, ContextType>): AsyncIterable<Quad> {
    const literalQuadSubject = DefaultDataFactory.fromTerm(options.literalQuad.subject)
    const literalQuadPredicate = options.literalQuad.predicate ? DefaultDataFactory.fromTerm(options.literalQuad.predicate) : ns.contains
    const literalQuadGraph = options.literalQuad.graph ? DefaultDataFactory.fromTerm(options.literalQuad.graph) : DefaultDataFactory.defaultGraph()

    if (!isQuadSubject(literalQuadSubject)) {
        throw new Error("Invalid subject for literal quad")
    }
    if (!isQuadPredicate(literalQuadPredicate)) {
        throw new Error("Invalid predicate for literal quad")
    }
    if (!isQuadGraph(literalQuadGraph)) {
        throw new Error("Invalid graph for literal quad")
    }

    const profileQuad: Quad | undefined = options.profileQuad ? DefaultDataFactory.quad(
        literalQuadSubject,
        typeof options.profileQuad === "boolean" ? ns.type : (options.profileQuad.predicate || ns.type),
        ns.typeUnknown,
        typeof options.profileQuad === "boolean" ? ns.type : (options.profileQuad.graph || literalQuadGraph),
    ) : undefined

    if (isQuad(source)) {
        yield* profile(ns.typeQuad)
        return yield source
    } else if (isQuadLike(source)) {
        // It is a quad & quad like, as it will be quad when returned
        yield* profile(ns.typeQuadLike)
        yield* profile(ns.typeQuad)
        return yield DefaultDataFactory.fromQuad(source)
    }

    const transformations: TransformFn[] = [
        ...(options.transformations || [])
    ]

    if (options.getQuad) {
        transformations.push(options.getQuad)
    }

    const completeOptions = {
        ...options,
        getQuad: undefined,
        transformations,
        literalQuad: {
            subject: literalQuadSubject,
            predicate: literalQuadPredicate,
            graph: literalQuadGraph
        },
        profileQuad: profileQuad ? {
            predicate: profileQuad.predicate,
            graph: profileQuad.graph
        } : undefined
    }

    for (const transformation of transformations) {
        const result = transformation(source, completeOptions)
        if (isAsyncIterable(result)) {
            let anyThing = false
            for await (const value of result) {
                anyThing = true
                yield value
            }
            if (anyThing) {
                return
            }
        }
    }

    if (isUnknown(source)) {
        yield* profile(ns.typeUnknown)
        if (!options.onUnknown) {
            return
        }
        const result = options.onUnknown(source)
        if (isUnknown(result)) {
            return
        }
        return yield* transform(result, options)
    } else if (typeof source === "function") {
        return yield* thing(ns.typeFunction, async function *(knownAs, options) {
            return yield* transform(source(knownAs, options), options)
        })
    } else if (isPromise(source)) {
        return yield* thing(ns.typePromise, async function *(knownAs, options) {
            return yield* transform(await source, options)
        })
    } else if (typeof source === "string") {
        return yield* thing(ns.typeString, async function *(knownAs) {
            yield DefaultDataFactory.quad(
                knownAs,
                literalQuadPredicate,
                DefaultDataFactory.literal(source, DefaultDataFactory.namedNode("http://www.w3.org/2001/XMLSchema#string")),
                literalQuadGraph
            )
        })
    } else if (typeof source === "number") {
        return yield* thing(ns.typeNumber, async function *(knownAs) {
            yield DefaultDataFactory.quad(
                literalQuadSubject,
                literalQuadPredicate,
                // Double because xsd defines double as 64 bit float, which is what js uses _double-precision 64-bit binary format IEEE 754 _
                DefaultDataFactory.literal(source.toString(), DefaultDataFactory.namedNode("http://www.w3.org/2001/XMLSchema#double")),
                literalQuadGraph
            )
        })
    } else if (typeof source === "bigint") {
        return yield* thing(ns.typeBigint, async function *(knownAs) {
            /**
             * xsd:positiveInteger	Integer numbers >0
             * xsd:nonNegativeInteger	Integer numbers ≥0
             * xsd:negativeInteger	Integer numbers <0
             * xsd:nonPositiveInteger	Integer numbers ≤0
             */
            const type = source >= 0n ? "nonNegativeInteger" : "nonPositiveInteger"
            yield* profile(ns.typeBigint)
            return yield DefaultDataFactory.quad(
                knownAs,
                literalQuadPredicate,
                DefaultDataFactory.literal(source.toString(), DefaultDataFactory.namedNode(`http://www.w3.org/2001/XMLSchema#${type}`)),
                literalQuadGraph
            )
        })
    } else if (typeof source === "boolean") {
        return yield* thing(ns.typeBoolean, async function *(knownAs) {
            yield DefaultDataFactory.quad(
                knownAs,
                literalQuadPredicate,
                DefaultDataFactory.literal(source.toString(), DefaultDataFactory.namedNode("http://www.w3.org/2001/XMLSchema#boolean")),
                literalQuadGraph
            )
        })
    } else if (source instanceof Date) {
        return yield* thing(ns.typeDate, async function *(knownAs) {
            yield DefaultDataFactory.quad(
                knownAs,
                literalQuadPredicate,
                // We drop the knowledge of the originating timezone here... this may be a problem for a small amount
                // of users, in their case, they're able to provide a literal directly
                DefaultDataFactory.literal(source.toISOString(), DefaultDataFactory.namedNode("http://www.w3.org/2001/XMLSchema#dateTimeStamp")),
                literalQuadGraph
            )
        })
    } else if (options.isBinaryType && options.isBinaryType(source)) {
        return yield* thing(ns.typeBinary, async function *(knownAs) {
            const hex = options.getHex && await options.getHex(source)
            const base64 = !hex && options.getBase64 && await options.getBase64(source)
            if (hex) {
                yield* profile(ns.typeHex)
            }
            if (hex) {
                yield* profile(ns.typeBase64)
            }
            const string = hex || base64
            if (string) {
                return yield DefaultDataFactory.quad(
                    knownAs,
                    literalQuadPredicate,
                    DefaultDataFactory.literal(string, DefaultDataFactory.namedNode(`http://www.w3.org/2001/XMLSchema#${hex ? "hexBinary" : "base64Binary"}`)),
                    literalQuadGraph
                )
            } else {
                throw new Error("isBinaryType returned true but both getHex and getBase64 returned undefined sources")
            }
        })

    } else if (isLiteral(source)) {
        return yield* thing(ns.typeLiteral, async function *(knownAs) {
            yield DefaultDataFactory.quad(
                knownAs,
                literalQuadPredicate,
                source,
                literalQuadGraph
            )
        })
    } else if (options.isLiteralType && options.isLiteralType(source) && options.getLiteral) {
        return yield* thing(ns.typeLiteral, async function *(knownAs) {
            return yield* transform(await options.getLiteral(source), options)
        })
    } else if (isLiteralLike(source) && isNamedNodeLike(source.datatype)) {
        return yield* thing(ns.typeLiteralLike, async function *(knownAs) {
            if (!isNamedNodeLike(source.datatype)) {
                throw new Error("We checked twice!")
            }
            return yield DefaultDataFactory.quad(
                knownAs,
                literalQuadPredicate,
                new Literal(
                    source.value,
                    source.language,
                    new NamedNode(
                        source.datatype.value
                    )
                ),
                literalQuadGraph
            )
        })
    } else if (isLiteralLike(source)) {
        throw new Error("isLiteralLike should pick up on datatype being required as a NamedNode as well")
    } else if (isIterable(source)) {
        return yield* thing(ns.typeIterable, async function *(knownAs, options) {
            for (const child of source) {
                for await (const quad of transform(child, options)) {
                    yield quad
                }
            }
        })
    } else if (isAsyncIterable(source)) {
        return yield* thing(ns.typeAsyncIterable, async function *(knownAs, options) {
            for await (const child of source) {
                for await (const quad of transform(child, options)) {
                    yield quad
                }
            }
        })
    } else  {
        return yield* thing(ns.typeJSON, async function *(knownAs) {
            return yield encode(
                knownAs,
                literalQuadPredicate,
                source,
                literalQuadGraph
            )
        })
    }

    function isUnknown(source: unknown): boolean {
        if (options.isUnknown && options.isUnknown(source)) {
            return true
        }
        return (
            (typeof source === "number" && isNaN(source)) ||
            typeof source === "undefined" ||
            source === null
        )
    }

    async function *thing<Thing>(thingType: NamedNode | BlankNode, fn: ThingFn) {
        const blankNode = DefaultDataFactory.blankNode()
        yield new Quad(
            literalQuadSubject,
            ns.is,
            blankNode,
            literalQuadGraph
        )
        yield* profile(thingType, blankNode)
        yield* fn(blankNode, {
            ...completeOptions,
            literalQuad: {
                ...completeOptions.literalQuad,
                subject: blankNode
            }
        })
    }

    async function *profile(type: NamedNode | BlankNode, knownAs?: NamedNode | BlankNode) {
        if (!profileQuad) {
            return
        }
        yield new Quad(
            knownAs || literalQuadSubject,
            profileQuad.predicate,
            type,
            profileQuad.graph
        )
    }
}
