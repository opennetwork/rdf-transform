import {
    DefaultDataFactory, isLiteral, isLiteralLike, isNamedNodeLike,
    isQuad, isQuadGraph,
    isQuadLike, isQuadPredicate, isQuadSubject, Literal, LiteralLike, NamedNode,
    Quad, QuadGraphLike, QuadPredicateLike,
    QuadSubjectLike
} from "@opennetwork/rdf-data-model"
import { isAsyncIterable, isIterable, isPromise } from "iterable"
import { encode } from "@opennetwork/rdf-json"
import * as ns from "./namespace"

export * from "./namespace"

export interface TransformOptions<LiteralType = unknown, BinaryType = unknown> {
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
    getQuad?(source: TransformableSource): void | Promise<void> | TransformableSource
}

export type TransformableAsyncIterableSource = AsyncIterable<TransformableSource>
export type TransformableIterableSource = Iterable<TransformableSource>
export type TransformablePromiseSource = Promise<TransformableSource>
export type TransformableFunctionSource = () => TransformableSource
export type TransformableSource =
    | TransformableAsyncIterableSource
    | TransformableIterableSource
    | TransformablePromiseSource
    | TransformableFunctionSource
    // Above is more documenting what we _expect_ but in the end we do not need to know the type of the source
    | unknown

export async function *transform<LiteralType = unknown, BinaryType = unknown>(source: TransformableSource, options: TransformOptions<BinaryType, LiteralType>): AsyncIterable<Quad> {
    let profileQuad: Quad | undefined = undefined

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

    if (isQuad(source)) {
        yield* profile(ns.typeQuad)
        return yield source
    } else if (isQuadLike(source)) {
        yield* profile(ns.typeQuadLike)
        return yield DefaultDataFactory.fromQuad(source)
    }

    if (options.getQuad) {
        const initialQuad = await options.getQuad(source)
        if (!isUnknown(initialQuad)) {
            yield* transform(initialQuad, options)
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
    } else if (isAsyncIterable(source)) {
        yield* profile(ns.typeAsyncIterable)
        let definedSelf = false
        const selfBlankNode = DefaultDataFactory.blankNode()
        for await (const child of source) {
            for await (const quad of transform(child, {
                ...options,
                literalQuad: {
                    ...options.literalQuad,
                    subject: selfBlankNode
                }
            })) {
                if (!definedSelf) {
                    yield new Quad(
                        literalQuadSubject,
                        literalQuadPredicate,
                        selfBlankNode,
                        literalQuadGraph
                    )
                    definedSelf = true
                }
                yield quad
            }
        }
        return
    } else if (isIterable(source)) {
        yield* profile(ns.typeIterable)
        let definedSelf = false
        const selfBlankNode = DefaultDataFactory.blankNode()
        for (const child of source) {
            for await (const quad of transform(child, {
                ...options,
                literalQuad: {
                    ...options.literalQuad,
                    subject: selfBlankNode
                }
            })) {
                if (!definedSelf) {
                    yield new Quad(
                        literalQuadSubject,
                        literalQuadPredicate,
                        selfBlankNode,
                        literalQuadGraph
                    )
                    definedSelf = true
                }
                yield quad
            }
        }
        return
    } else if (typeof source === "function") {
        yield* profile(ns.typeFunction)
        return yield *transform(source(), options)
    } else if (isPromise(source)) {
        yield* profile(ns.typePromise)
        return yield* transform(await source, options)
    } else if (typeof source === "string") {
        yield* profile(ns.typeString)
        return yield DefaultDataFactory.quad(
            literalQuadSubject,
            literalQuadPredicate,
            DefaultDataFactory.literal(source, DefaultDataFactory.namedNode("http://www.w3.org/2001/XMLSchema#string")),
            literalQuadGraph
        )
    } else if (typeof source === "number") {
        yield* profile(ns.typeNumber)
        return yield DefaultDataFactory.quad(
            literalQuadSubject,
            literalQuadPredicate,
            // Double because xsd defines double as 64 bit float, which is what js uses _double-precision 64-bit binary format IEEE 754 _
            DefaultDataFactory.literal(source.toString(), DefaultDataFactory.namedNode("http://www.w3.org/2001/XMLSchema#double")),
            literalQuadGraph
        )
    } else if (typeof source === "bigint") {
        /**
         * xsd:positiveInteger	Integer numbers >0
         * xsd:nonNegativeInteger	Integer numbers ≥0
         * xsd:negativeInteger	Integer numbers <0
         * xsd:nonPositiveInteger	Integer numbers ≤0
         */
        const type = source >= 0n ? "nonNegativeInteger" : "nonPositiveInteger"
        yield* profile(ns.typeBigint)
        return yield DefaultDataFactory.quad(
            literalQuadSubject,
            literalQuadPredicate,
            DefaultDataFactory.literal(source.toString(), DefaultDataFactory.namedNode(`http://www.w3.org/2001/XMLSchema#${type}`)),
            literalQuadGraph
        )
    } else if (typeof source === "boolean") {
        yield* profile(ns.typeBoolean)
        return yield DefaultDataFactory.quad(
            literalQuadSubject,
            literalQuadPredicate,
            DefaultDataFactory.literal(source.toString(), DefaultDataFactory.namedNode("http://www.w3.org/2001/XMLSchema#boolean")),
            literalQuadGraph
        )
    } else if (source instanceof Date) {
        yield* profile(ns.typeDate)
        return yield DefaultDataFactory.quad(
            literalQuadSubject,
            literalQuadPredicate,
            // We drop the knowledge of the originating timezone here... this may be a problem for a small amount
            // of users, in their case, they're able to provide a literal directly
            DefaultDataFactory.literal(source.toISOString(), DefaultDataFactory.namedNode("http://www.w3.org/2001/XMLSchema#dateTimeStamp")),
            literalQuadGraph
        )
    } else if (options.isBinaryType && options.isBinaryType(source)) {
        yield* profile(ns.typeBinary)
        const hex = options.getHex && await options.getHex(source)
        if (hex) {
            yield* profile(ns.typeHex)
        }
        const base64 = !hex && options.getBase64 && await options.getBase64(source)
        if (hex) {
            yield* profile(ns.typeBase64)
        }
        const string = hex || base64
        if (string) {
            return yield DefaultDataFactory.quad(
                literalQuadSubject,
                literalQuadPredicate,
                DefaultDataFactory.literal(string, DefaultDataFactory.namedNode(`http://www.w3.org/2001/XMLSchema#${hex ? "hexBinary" : "base64Binary"}`)),
                literalQuadGraph
            )
        } else {
            throw new Error("isBinaryType returned true but both getHex and getBase64 returned undefined sources")
        }
    } else if (isLiteral(source)) {
        yield* profile(ns.typeLiteral)
        return yield DefaultDataFactory.quad(
            literalQuadSubject,
            literalQuadPredicate,
            source,
            literalQuadGraph
        )
    } else if (options.isLiteralType && options.isLiteralType(source) && options.getLiteral) {
        yield* profile(ns.typeLiteral)
        return yield* transform(await options.getLiteral(source), options)
    } else if (isLiteralLike(source) && isNamedNodeLike(source.datatype)) {
        yield* profile(ns.typeLiteralLike)
        return yield DefaultDataFactory.quad(
            literalQuadSubject,
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
    } else if (isLiteralLike(source)) {
        throw new Error("isLiteralLike should pick up on datatype being required as a NamedNode as well")
    } else {
        yield* profile(ns.typeJSON)
        return yield encode(
            literalQuadSubject,
            literalQuadPredicate,
            source,
            literalQuadGraph
        )
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

    async function *profile(type: NamedNode) {
        if (!options.profileQuad) {
            return
        }
        if (!profileQuad) {
            profileQuad = DefaultDataFactory.quad(
                literalQuadSubject,
                typeof options.profileQuad === "boolean" ? ns.type : (options.profileQuad.predicate || ns.type),
                type,
                typeof options.profileQuad === "boolean" ? ns.type : (options.profileQuad.graph || literalQuadGraph),
            )
        }
        yield new Quad(
            profileQuad.subject,
            profileQuad.predicate,
            type,
            profileQuad.graph
        )
    }
}
